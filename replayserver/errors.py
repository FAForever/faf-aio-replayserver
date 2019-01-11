class BadConnectionError(Exception):
    """
    Superclass for things a connection can do wrong. Thrown in connection
    handling code, used to clean up any resources and the connection itself.
    """
    def type_name(self):
        return type(self).__name__


class MalformedDataError(BadConnectionError):
    """
    Used for ill-formed connections, including ones that end prematurely.
    """
    pass


class CannotAcceptConnectionError(BadConnectionError):
    """
    Used whenever the connection is well-formed, but cannot be accepted
    (e.g. a reader connection for a nonexistent replay, or a connection made
    when the server is closing)
    """
    pass


class EmptyConnectionError(BadConnectionError):
    """
    Reserved for connections that never wrote anything. We don't log these,
    since they happen whenever somebody joined a game and left without that
    game starting.
    """
    pass


class BookkeepingError(Exception):
    """
    Used by Bookkeeper to signify that something went wrong when saving a
    replay.
    """
    pass
