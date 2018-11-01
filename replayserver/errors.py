class BadConnectionError(Exception):
    """Superclass for things a connection can do wrong."""
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


class BookkeepingError(Exception):
    pass
