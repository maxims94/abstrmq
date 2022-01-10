from ..future_queue_session import FutureQueueSession
from ..future_queue import FutureQueue
from ..publisher import BasicPublisher, DirectPublisher
from ..exceptions import RemoteError
from ..state_condition import StateCondition
from asyncio import Event
from enum import Enum
import logging

log = logging.getLogger(__name__)

class InteractiveSessionError(Exception):
  pass

class InteractiveSessionState(Enum):
  INIT = 'init'
  START = 'start'
  RUNNING = 'running'
  CLOSED = 'close'

"""
Implements full-duplex communication between client and server
"""

class InteractiveClientSession(FutureQueueSession):

  START_REPLY_TIMEOUT = 2

  def __init__(self, reply_queue: FutureQueue):
    super().__init__(reply_queue)

  async def publish_start(self, body, publisher: BasicPublisher):
    """
    Initiate a session by sending a request to the server. reply_to is added automatically

    :raises: TimeoutError, InteractiveSessionError
    """
    self.generate_corr_id()
    self.publisher = publisher
    publisher.reply_to = self.queue.name

    await self.publish(body)

    # TODO
    # self.state = START

    msg = await self.receive(timeout=self.START_REPLY_TIMEOUT)

    try:
      msg.assert_reply_to()
      msg.assert_corr_id()
    except RemoteError as ex:
      raise InteractiveSessionError(repr(ex))

    if '_session' not in msg.content:
      raise InteractiveSessionError("Malformed reply")

    state = msg.content['_session']

    if state == 'start_success':
      self.publisher = DirectPublisher(msg.ch, msg.reply_to)

    elif state == 'start_failure':
      if '_message' not in msg.content:
        raise InteractiveSessionError("Malformed reply")
      raise InteractiveSessionError(f"Session rejected: {msg.content['_message']}")
    else:
      raise InteractiveSessionError("Malformed reply")

class InteractiveServerSession(FutureQueueSession):

  def __init__(self, reply_queue: FutureQueue):
    super().__init__(reply_queue)
    self.state = StateCondition(InteractiveSessionState, InteractiveSessionState.INIT)
  
  async def receive_start(self, *args, validator=None, **kwargs):
    """
    Wait for the client to initiate a new session.

    :param validator: expected to throw RemoteError if invalid
    :raises: TimeoutError
    """

    msg = await self.receive(*args, **kwargs)

    await self.state.set(InteractiveSessionState.START)

    try:
      msg.assert_reply_to()
      msg.assert_corr_id()
    except RemoteError as ex:
      # Unless we have reply_to and corr_id, we can't even send back an error message
      log.warning(repr(ex))
      await self.state.set(InteractiveSessionState.CLOSED)
      return

    self.publisher = DirectPublisher(msg.ch, msg.reply_to, reply_to=self.queue.name)
   
    try:
      if validator:
        validator(msg)
    except RemoteError as ex:
      await self.publish({'_session': 'start_failure', '_message': str(ex)})
      await self.state.set(InteractiveSessionState.CLOSED)
      return

    await self.publish({'_session': 'start_success'})
    await self.state.set(InteractiveSessionState.RUNNING)

  async def started(self):
    await self.state.wait_for(InteractiveSessionState.RUNNING, InteractiveSessionState.CLOSED)
