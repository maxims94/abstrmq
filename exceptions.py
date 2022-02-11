class UserInterruptError(Exception):
  """
  Any exception caused by user intervention, e.g. abort
  """
  pass

class RemoteError(Exception):
  """
  Any error that was caused by the remote host

  Example: invalid server reply, invalid client request
  """
  pass

class InvalidMessageError(RemoteError):
  """
  An invalid, protocol-violating message was received
  """
  pass

class InvalidReplyError(InvalidMessageError):
  """
  The host replied with an invalid message
  """
  def __init__(self, message, expected=None, actual=None):
    super().__init__(message)
    self.expected = expected
    self.actual = actual

  def __str__(self):
    return f"{super().__str__()}: Expected '{self.expected}', but got '{self.actual}'"

class RemoteUnavailableError(RemoteError):
  """
  The remote host doesn't respond
  """
  pass
