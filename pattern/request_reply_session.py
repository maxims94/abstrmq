from ..future_queue_session import FutureQueueSession
from ..future_queue import FutureQueue
from ..publisher import BasicPublisher, DirectPublisher
from ..exceptions import InvalidMessageError

"""
Protocol:

A client makes a request to a server
It provides a reply queue and a correlation ID
The server receives the request
It processes the request and generates a reply
Then, it sends a reply back to the client
This ends the session on both sides
"""

class RequestReplyClientSession(FutureQueueSession):
  """
  Usage:

  session = RequestReplyClientSession(reply_queue, publisher)
  try:
    await session.publish_request(body)
    reply = await session.receive_reply(validator=func)
  except RemoteError:
    print("invalid client request")
  except asyncio.TimeoutError:
    print("timeout")
  except asyncio.CancelledError:
    print("cancelled")
  """

  PUBLISH_REQUEST_TIMEOUT = 1
  RECEIVE_REPLY_TIMEOUT = 3

  def __init__(self, reply_queue: FutureQueue, publisher: BasicPublisher):
    super().__init__(reply_queue, publisher)
    self.generate_corr_id()
    self.publisher.reply_to = self.queue.name

  async def publish_request(self, *args, **kwargs):

    if 'timeout' not in kwargs:
      kwargs['timeout'] = self.PUBLISH_REQUEST_TIMEOUT

    await super().publish(*args, **kwargs)

  async def receive_reply(self, validator=None, timeout=None):
    """
    Waits for the server's reply. Since this is the client, a timeout is required! If none is provided, a default one is used

    If the server replies within the given timeframe, the reply is validated through a custom validator.

    If it is valid, the reply is returned.

    :validator: a callable that takes a QueueMessage and validates it; raises RemoteError if a requirement is violated
    :raises: TimeoutError, CancelledError, RemoteError
    :rtype: QueueMessage
    """
    timeout = timeout or self.RECEIVE_REPLY_TIMEOUT
    msg = await super().receive(timeout=timeout)
    if validator:
      validator(msg)
    return msg

class RequestReplyServerSession(FutureQueueSession):
  """
  Validator: It only checks the format of the request, it does NOT need to guarantee that its associated operation will be successful!

  Usage:

  session = RequestReplyServerSession(request_queue)
  try:
    request = await session.receive_request(filter, validator=func)
    reply = func(request)
    await session.publish_reply(reply)
  except RemoteError:
    print("invalid client request")
  except asyncio.TimeoutError:
    print("timeout")
  except asyncio.CancelledError:
    print("cancelled")
  """

  def __init__(self, request_queue: FutureQueue):
    super().__init__(request_queue)

  async def receive_request(self, *args, validator=None, **kwargs):
    """
    Wait for a request from the client.

    Then, validate the request. If it fails, a RemoteError is thrown.
    
    If it is valid, return the request. It is expected that the server will process it now.

    :rtype: QueueMessage
    :param validator: a callable that checks whether a request has the right format. If not, it is expected to raise a RemoteError

    :raises: RemoteError
    """
    msg = await self.receive(*args, **kwargs)

    msg.assert_reply_to()
    msg.assert_corr_id()

    if validator:
      validator(msg)

    self.publisher = DirectPublisher(msg.ch, msg.reply_to)

    return msg

  async def publish_reply(self, *args, **kwargs):
    """
    Publish reply to client

    :raises: TimeoutErrro
    """

    await self.publish(*args, **kwargs)
