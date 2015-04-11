from setuptools import setup
import py2exe

setup (
    name = "RabbitRequeue",
    version = "0.1",
    description="RabbitRequeue is a utility for re-queueing RabbitMQ messages.",
    author="Greg Major",
    author_email="", # Removed to limit spam harvesting.
    url="http://www.leadpipesoftware.com/",
    packages=['RabbitRequeue'],
    entry_points = {
        'console_scripts': ['RabbitRequeue = RabbitRequeue.__main__:main']
                    },
    download_url = "http://www.leadpipesoftware.com/",
    zip_safe = True
)
