"""Wrapper around the publish features of stomp.py.

The wrapper adds asynchronicity and connection management.

Changes in version 2.1 (development):
- Wait for successful connection before dequeuing message.
- Always attempt disconnection when publisher thread stops.

Changes in version 2.0:
- repackage so source root is not the same as svn root.
- update to use stomp.py version 2.0.4.
- publisher.send() takes separate arguments not tuple.
- publisher.send() allows headers and keyword_headers
- publisher.stop() only causes the publisher thread to die when the local queue
    is empty.
- provide publisher.addExitHandler() so that a client can optionally tell the
    publisher to attempt to empty the local queue, with a timeout, before dying.
- publisher disconnects if idle too long (configurable).
- add _publisher_timestamp as header in publisher.send() (i.e. not in message)
- publisher.send() simply passes the message body onto stomp.py, instead of
    accepting only dict and converting to string using repr().
"""

from publisher import createPublisher

__version__ = '2.0'
