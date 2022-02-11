import asyncio
from ..future_queue_session import FutureQueueSession
from ..future_queue import FutureQueue
from ..publisher import BasicPublisher, DirectPublisher
from ..exceptions import InvalidMessageError
from ..managed_queue import ManagedQueue

from aiormq.exceptions import PublishError

import logging
log = logging.getLogger(__name__)

# TODO: use `mandatory`

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


  ex = HeadersExchange(self._ch, exchange)
  await ex.declare()
  pub = HeadersExchangePublisher(self._ch, exchange)

  session = RequestReplyClientSession(reply_queue, pub)
  try:
    await session.publish_request(body)
    reply = await session.receive_reply(validator=func)
  except RemoteError:
    print("Invalid server reply")
  except asyncio.TimeoutError:
    print("timeout")
  except asyncio.CancelledError:
    print("cancelled")
  """

  PUBLISH_REQUEST_TIMEOUT = 1
  RECEIVE_REPLY_TIMEOUT = 3

  def __init__(self, reply_queue: FutureQueue, publisher: BasicPublisher):
    super().__init__(reply_queue, publisher)
    self.set_corr_id()
    self.publisher.reply_to = self.queue.name
    self._register_fut = None

  async def publish_request(self, *args, **kwargs):
    """
    :raises: TimeoutError, PublishError
    """

    if isinstance(self.queue, ManagedQueue):
      # corr_id is already set
      self._register_fut = self.register()

    if 'timeout' not in kwargs:
      kwargs['timeout'] = self.PUBLISH_REQUEST_TIMEOUT

    if 'mandatory' not in kwargs:
      kwargs['mandatory'] = True

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
    try:
      msg = await super().receive(timeout=timeout)
      if validator:
        validator(msg)
      return msg
    except (asyncio.CancelledError, Exception) as ex:
      raise ex
    finally:
      if self._register_fut is not None:
        self.deregister(self._register_fut)

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

  ManagedQueue
  * Registration must be done outside of this session!
  * Reason: The request is registered as long as receive_request is running. Now consider a session loop that just received a message and is currently processing it. receive_request is not active, so the Future is not registered, so further requests will be dropped instead of queued!
  """

  def __init__(self, request_queue: FutureQueue):
    super().__init__(request_queue)
    self._started = asyncio.Event()

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

    self._started.set()

    msg.assert_reply_to()
    msg.assert_corr_id()

    self.publisher = DirectPublisher(msg.ch, msg.reply_to)

    if validator:
      validator(msg)

    return msg

  async def publish_reply(self, *args, **kwargs):
    """
    Publish reply to client

    :raises: TimeoutError, PublishError
    """

    if 'mandatory' not in kwargs:
      kwargs['mandatory'] = True

    await self.publish(*args, **kwargs)

  async def started(self):
    await self._started.wait()
