""" RabbitRequeue: Requeues messages in the RabbitMQ error queue. """

import sys
import argparse
import json
import requests
import pprint

# Authorization String Examples
#
# guest/guest is 'Basic Z3Vlc3Q6Z3Vlc3Q='
# rabbit/rabbit is 'Basic cmFiYml0OnJhYmJpdA=='

# Define the headers to strip out before replaying a message...
nservicebus_runtime_headers = ['NServiceBus.FLRetries', 'NServiceBus.Retries']
nservicebus_diagnostic_headers = ['$.diagnostics.originating.hostid', '$.diagnostics.hostdisplayname', '$.diagnostics.hostid', '$.diagnostics.license.expired']
nservicebus_audit_headers = ['NServiceBus.Version', 'NServiceBus.TimeSent', 'NServiceBus.EnclosedMessageTypes', 'NServiceBus.ProcessingStarted', 'NServiceBus.ProcessingEnded', 'NServiceBus.OriginatingAddress', 'NServiceBus.ProcessingEndpoint', 'NServiceBus.ProcessingMachine']
nservicebus_error_headers = ['NServiceBus.FailedQ']

def main(args=None):
    """ The program entry point. """

    parser = argparse.ArgumentParser(description='Gets messages from RabbitMQ and stores them in RavenDB.')

    parser.add_argument('rabbit_source_queue', help='the name of the RabbitMQ source queue to get the messages from')
    parser.add_argument('message_count', help='the number of messages to requeue')

    parser.add_argument('-d', '--rabbit_destination_queue', help='the name of the RabbitMQ destination queue')
    parser.add_argument('-r', '--rabbit_host_url', default='http://localhost', help='the RabbitMQ host URL')
    parser.add_argument('-p', '--rabbit_port', type=int, default=15672, help='the RabbitMQ port')
    parser.add_argument('-s', '--rabbit_vhost', default='%2F', help='the RabbitMQ vhost name')
    parser.add_argument('-z', '--rabbit_authorization_string', default='Basic Z3Vlc3Q6Z3Vlc3Q=', help='the authorization string for the RabbitMQ request header')
    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()

    # Get the RabbitMQ messages...
    messages = get_rabbit_messages(args.message_count, args.rabbit_host_url, args.rabbit_port, args.rabbit_vhost, args.rabbit_source_queue, args.rabbit_authorization_string, args.verbose)

    # Requeue the messages...
    requeue_messages(messages, args.rabbit_host_url, args.rabbit_port, args.rabbit_vhost, args.rabbit_authorization_string, args.rabbit_destination_queue, args.verbose)


def build_rabbit_get_url(rabbit_host_url, rabbit_port, rabbit_vhost, rabbit_source_queue, verbose=False):
    """ Builds the RabbitMQ URL.

    Args:
      rabbit_host_url (str): The RabbitMQ host URL.
      rabbit_port (int): The RabbitMQ host port.
      rabbit_vhost (str): The RabbitMQ vhost.
      rabbit_source_queue (str): The name of the RabbitMQ source queue.
      verbose (bool): Determines if verbose output is displayed.

    Returns:
      str: A fully-constructed URL for RabbitMQ.
    """

    url = rabbit_host_url + ':' + str(rabbit_port) + '/api/queues/' + rabbit_vhost + '/' + rabbit_source_queue + '/get'

    if verbose:
        print(url)

    return url


def build_rabbit_publish_url(rabbit_host_url, rabbit_port, rabbit_vhost, rabbit_destination_queue, verbose=False):
    """ Builds the RabbitMQ URL.

    Args:
      rabbit_host_url (str): The RabbitMQ host URL.
      rabbit_port (int): The RabbitMQ host port.
      rabbit_vhost (str): The RabbitMQ vhost.
      rabbit_source_queue (str): The name of the RabbitMQ source queue.
      verbose (bool): Determines if verbose output is displayed.

    Returns:
      str: A fully-constructed URL for RabbitMQ.
    """

    url = rabbit_host_url + ':' + str(rabbit_port) + '/api/exchanges/' + rabbit_vhost + '/' + rabbit_destination_queue + '/publish'

    if verbose:
        print(url)

    return url


def get_rabbit_messages(message_count, rabbit_host_url, rabbit_port, rabbit_vhost, rabbit_source_queue, rabbit_authorization_string, verbose=False):
    """ Gets messages from RabbitMQ.

    Args:
      message_count (int): The number of messages to get.
      rabbit_host_url (str): The RabbitMQ host URL.
      rabbit_port (int): The RabbitMQ host port.
      rabbit_vhost (str): The name of the RabbitMQ vhost.
      rabbit_source_queue (str): The name of the RabbitMQ source queue.
      rabbit_authorization_string (str): The authorization string for the request header.

    Returns:
      list: A list of the requested messages.
    """

    print("Getting messages from {0}...".format(rabbit_source_queue))

    rabbit_url = build_rabbit_get_url(rabbit_host_url, rabbit_port, rabbit_vhost, rabbit_source_queue)
    rabbit_request_data = {'count': message_count, 'requeue': 'false', 'encoding': 'auto'}
    rabbit_request_json = json.dumps(rabbit_request_data)
    rabbit_request_headers = {'Content-type': 'application/json', 'Authorization': rabbit_authorization_string}

    rabbit_response = requests.post(rabbit_url, data=rabbit_request_json, headers=rabbit_request_headers)

    if rabbit_response.status_code != 200:
        pprint.pprint(rabbit_response.status_code)
        sys.exit('ERROR (' + str(rabbit_response.status_code) + '): ' + rabbit_response.text)

    return rabbit_response.json()


def requeue_messages(messages, rabbit_host_url, rabbit_port, rabbit_vhost, rabbit_authorization_string, destination_queue=None, verbose=False):
    """ Requeues messages in RabbitMQ.

    Args:
      messages (list): The messages to store in RavenDB.
      rabbit_host_url (str): The RabbitMQ host URL.
      rabbit_port (int): The RabbitMQ host port.
      rabbit_vhost (str): The name of the RabbitMQ vhost.
      rabbit_authorization_string (str): The authorization string for the request header.
      verbose (bool): Determines if verbose output is displayed.

    Returns:
      int: The number of messages requeued.
    """

    rabbit_request_headers = {'Content-type': 'application/json', 'Authorization': rabbit_authorization_string}

    processed_messages = 0

    for message in messages:

        processed_messages += 1

        if destination_queue == None:
            destination_queue = get_source_queue(message)

        print("{0} of {1} - Requeueing message to {2}".format(processed_messages, len(messages), destination_queue))

        message = scrub_message(message, nservicebus_runtime_headers)
        message = scrub_message(message, nservicebus_diagnostic_headers)
        message = scrub_message(message, nservicebus_audit_headers)
        message = scrub_message(message, nservicebus_error_headers)

        rabbit_url = build_rabbit_publish_url(rabbit_host_url, rabbit_port, rabbit_vhost, destination_queue)

        if verbose:
            print(rabbit_url)

        json_message = json.dumps(message)

        rabbit_response = requests.post(rabbit_url, data=json_message, headers=rabbit_request_headers)

        if rabbit_response.status_code != 200:
            pprint.pprint(rabbit_response.status_code)
            sys.exit('ERROR (' + str(rabbit_response.status_code) + '): ' + rabbit_response.text)

    return processed_messages


def get_source_queue(message):
    """ Gets the source queue from an NServiceBus message.

    Args:
      message (str): The message as a JSON string.

    Returns:
      str: The name of the source queue.
    """

    source_queue = None

    try:
        source_queue = message['properties']['headers']['NServiceBus.FailedQ']
    except KeyError:
        raise ValueError('The source queue could not be determined!')

    return source_queue.split('@', 1)[0]


def scrub_message(message, elements_to_delete):
    """ Scrubs the unnecesary header information out of a RabbitMQ message.

    Args:
      message (str): The RabbitMQ message in JSON format.
      elements_to_delete (list): A list of keys identifying the elements to delete.

    Returns:
      str: The scrubbed message in JSON format.
    """

    # NOTE: This whole function is really nothing more than a generic way to remove keys from a dictionary.

    for element_to_delete in elements_to_delete:

        try:
            del message[element_to_delete]
        except KeyError:
            pass

    for value in message.values():

        if isinstance(value, dict):

            scrub_message(value, elements_to_delete)

    return message


if __name__ == "__main__":
    main()
