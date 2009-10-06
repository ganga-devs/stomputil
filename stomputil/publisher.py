"""Wrapper around the publish features of stomp.py."""

import logging
import stomp
import time


BEAT_TIME = 0.3 # Seconds between publisher thread heart beats.
IDLE_TIMEOUT = 30 # Maximum seconds to idle before closing connection.
EXIT_TIMEOUT = 5 # Maximum seconds to clear queued messages on exit.
PUBLISHER_TIMESTAMP_HEADER = '_publisher_timestamp' # The publisher timestamp header name


class LoggerListener(stomp.ConnectionListener):
    """Connection listener which logs STOMP events."""

    def __init__(self, logger):
        self.logger = logger 

    def on_connecting(self, host_and_port):
        self.logger.debug('TCP/IP connected host=%s:%s.' % host_and_port)

    def on_connected(self, headers, body):
        self._log_frame('CONNECTED', headers, body)

    def on_disconnected(self):
        self.logger.warning('TCP/IP connection lost.')

    def on_message(self, headers, body):
        self._log_frame('MESSAGE', headers, body)

    def on_receipt(self, headers, body):
        self._log_frame('RECEIPT', headers, body)

    def on_error(self, headers, body):
        self._log_frame('ERROR', headers, body)

    def _log_frame(self, frame_type, headers, body):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug('STOMP %s frame received headers=%s body=%s.' % (frame_type, headers, body))


def createPublisher(T, server, port, user='', password='', logger=None,
                    idle_timeout=IDLE_TIMEOUT):
    """Create a new asynchronous publisher for sending messages to an MSG server.
    
    @param T: The thread class from which the publisher should inherit.
    @param server: The server host name.
    @param user: The user name.
    @param password: The password.
    @param logger: The logger instance.
    @param idle_timeout: Maximum seconds to idle before closing connection.
            Negative value indicates never close connection.
    
    The send method adds messages to a local queue, and the publisher thread sends
    them to the MSG server to emulate fast asynchronous sending of messages.
    
    Usage example with regular thread::
        from threading import Thread
        p = createPublisher(Thread, 'gridmsg001.cern.ch', 6163)
        p.start()
        p.addExitHandler()
        p.send('/topic/ganga.dashboard.test', 'Hello World!')
    
    Usage example with managed thread::
        #All GangaThread are registered with GangaThreadPool
        from Ganga.Core.GangaThread import GangaThread
        p = createPublisher(GangaThread, 'gridmsg001.cern.ch', 6163)
        p.start()
        p.send('/topic/ganga.dashboard.test', 'Hello World!')
        #GangaThreadPool calls p.stop() during shutdown
    """

    class AsyncStompPublisher(T):
        """Asynchronous asynchronous publisher for sending messages to an MSG server."""

        def __init__(self):
            T.__init__(self, name='AsyncStompPublisher')
            self.setDaemon(True)
            self.__should_stop = False
            self.__sending = False # indicates that publisher is currently sending
            # create connection
            self.connection = stomp.Connection([(server, port)], user, password)
            self.logger = logger
            # add logger listener to connection
            if logger is not None:
                self.connection.set_listener('logger', LoggerListener(logger))
            self.idle_timeout = idle_timeout
            # create queue to hold (message, headers, keyword_headers) tuples
            from Queue import Queue
            self.message_queue = Queue()

        def send(self, destination=None, message='', headers=None, **keyword_headers):
            """Add message to local queue for sending to MSG server.
            
            @param destination: An MSG topic or queue, e.g. /topic/dashboard.test.
            @param message: A string or dictionary of key-value pairs.
            @param headers: A dictionary of headers.
            @param keyword_headers: A dictionary of headers defined by keyword.
            
            If destination is not None, then it is added to keyword_headers.
            If not already present, then _publisher_timestamp (time since the Epoch
            (00:00:00 UTC, January 1, 1970) in seconds) is added to keyword_headers.
            Finally headers and keyword_headers are passed to stomp.py, which merges them.
            N.B. keyword_headers take precedence over headers.
            """
            if headers is None:
                headers = {}
            if destination is not None:
                keyword_headers['destination'] = destination
            if not PUBLISHER_TIMESTAMP_HEADER in keyword_headers:
                keyword_headers[PUBLISHER_TIMESTAMP_HEADER] = time.time()
            m = (message, headers, keyword_headers)
            self._log(logging.DEBUG, 'Queuing message %s with headers %s and keyword_headers %s.' % m)
            self.message_queue.put(m)

        def _send(self, (message, headers, keyword_headers)):
            """Send given message to MSG server."""
            self._log(logging.DEBUG, 'Sending message %s with headers %s and keyword_headers %s.' % (message, headers, keyword_headers))
            self.connection.send(message, headers, **keyword_headers)

        def _connect(self):
            """Connects to MSG server if not already connected."""
            if not self.connection.is_connected():
                self._log(logging.DEBUG, 'Connecting.')
                self.connection.start()
                self.connection.connect(wait=True)

        def _disconnect(self):
            """Disconnects from MSG server if not already disconnected."""
            if self.connection.is_connected():
                self._log(logging.DEBUG, 'Disconnecting.')
                self.connection.stop()

        def run(self):
            """Send messages, connecting as necessary and disconnecting after idle_timeout
            seconds.
            """
            idle_time = 0
            # keep running unless should_stop and queue empty
            while not (self.should_stop() and self.message_queue.empty()):
                # send while queue not empty
                while not self.message_queue.empty():
                    try:
                        self.__sending = True
                        m = self.message_queue.get()
                        self._connect()
                        self._send(m)
                    finally:
                        self.__sending = False
                    idle_time = 0
                # heart beat pause
                time.sleep(BEAT_TIME)
                idle_time += BEAT_TIME
                # disconnect if idle_timeout exceeded
                if idle_time >= self.idle_timeout > -1:
                    self._disconnect()

        def should_stop(self):
            """Indicates whether stop() has been called."""
            return self.__should_stop

        def stop(self):
            """Tells the publisher thread to stop gracefully.
            
            Typically called on a managed thread such as GangaThread.
            """
            if not self.__should_stop:
                self._log(logging.DEBUG, "Stopping: %s" % self.getName())
                self.__should_stop = True

        def addExitHandler(self, timeout=EXIT_TIMEOUT):
            """Adds an exit handler that allows the publisher up to timeout seconds to send
            queued messages.
            
            @param timeout: Maximum seconds to clear message queue on exit.
                    Negative value indicates clear queue without timeout.
            
            Typically used on unmanaged threads, i.e. regular Thread not GangaThread.
            """
            # register atexit finalize method
            import atexit
            atexit.register(self._finalize, timeout)

        def _finalize(self, timeout):
            """Allow the publisher thread up to timeout seconds to send queued messages.
            
            @param timeout: Maximum seconds to clear message queue on exit.
                    Negative value indicates clear queue without timeout.
            """
            self._log(logging.DEBUG, 'Finalizing.')
            finalize_time = 0
            while not self.message_queue.empty() or self.__sending:
                if finalize_time >= timeout > -1:
                    break
                time.sleep(BEAT_TIME)
                finalize_time += BEAT_TIME
            self._log(logging.DEBUG, 'Finalized after %s second(s). Local queue size is %s.' % (finalize_time, self.message_queue.qsize()))

        def _log(self, level, msg):
            """Log message if logger is defined."""
            if self.logger is not None and self.logger.isEnabledFor(level):
                self.logger.log(level, msg)

    return AsyncStompPublisher()


# for testing purposes
if __name__ == '__main__':
    l = logging.getLogger()
    from threading import Thread
    p = createPublisher(Thread, 'gridmsg001.cern.ch', 6163, logger=l, idle_timeout=5)
    p.start()
    p.addExitHandler(5)
    p.send('/topic/ganga.dashboard.test', 'Hello World 1!')
    p.send('/topic/ganga.dashboard.test', 'Hello World 2!')
    time.sleep(10)
    p.send('/topic/ganga.dashboard.test', 'Hello World 3!', {'foo1': 'bar1'}, foo2='bar2')
    p.send('/topic/ganga.dashboard.test', 'Hello World 4!', foo2='bar2')
    p.send('/topic/ganga.dashboard.test', repr({'msg': 'Hello World 5!'}))
