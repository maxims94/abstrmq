from ..future_queue_session import FutureQueueSession
from ..future_queue import FutureQueue
from ..publisher import BasicPublisher, DirectPublisher
from ..exceptions import RemoteError
from ..state_condition import StateCondition
import asyncio
from asyncio import Event
from enum import Enum
from contextlib import suppress
import logging

log = logging.getLogger(__name__)

"""
Full-duplex communication between client and server
"""

class InteractiveSessionError(Exception):
  pass

class InteractiveSessionClosed(InteractiveSessionError):
  """
  The remote host closed the session
  """
  pass

class InteractiveSessionState(Enum):
  INIT = 'init'
  START = 'start'
  RUNNING = 'running'
  CLOSE = 'close'

class InteractiveSessionBase(FutureQueueSession):

  def __init__(self, reply_queue: FutureQueue):
    super().__init__(reply_queue)
    self.state = StateCondition(InteractiveSessionState, InteractiveSessionState.INIT)

  async def publish_message(self, *args, **kwargs):
    """
    In running state, publish a message to the other host

    :raises: TimeoutError
    """
    assert self.state.is_in(InteractiveSessionState.RUNNING)
    await self.publish(*args, **kwargs)

  async def receive_message(self, *args, **kwargs):
    """
    In running state, receive a message from the other host

    :raises: InteractiveSessionClosed in case the other host ended the session
    """
    assert self.state.is_in(InteractiveSessionState.RUNNING)
    msg = await self.receive(*args, **kwargs)

    if msg.content == {'_session': 'close'}:
      raise InteractiveSessionClosed()
    return msg

  async def publish_close(self):
    """
    Host closes session on its end and notifies the remote host
    """
    assert not self.state.is_in(InteractiveSessionState.CLOSE)

    await self.state.set(InteractiveSessionState.CLOSE)

    with suppress(asyncio.TimeoutError):
      await self.publish({'_session': 'close'})

  def started(self):
    return self.state.wait_for(InteractiveSessionState.RUNNING, InteractiveSessionState.CLOSE)

  def closed(self):
    return self.state.wait_for(InteractiveSessionState.CLOSE)

#
# Client
#

class InteractiveClientSession(InteractiveSessionBase):

  START_REPLY_TIMEOUT = 2

  async def publish_start(self, body, publisher: BasicPublisher):
    """
    Initiate a session by sending a request to the server. reply_to is added automatically

    :raises: TimeoutError, InteractiveSessionError
    """

    assert self.state.is_in(InteractiveSessionState.INIT)

    self.generate_corr_id()
    self.publisher = publisher
    publisher.reply_to = self.queue.name

    await self.publish(body)

    await self.state.set(InteractiveSessionState.START)

    msg = await self.receive(timeout=self.START_REPLY_TIMEOUT)

    try:
      msg.assert_reply_to()
      msg.assert_corr_id()
    except RemoteError as ex:
      await self.state.set(InteractiveSessionState.CLOSE)
      raise InteractiveSessionError(f"Malformed reply: {repr(ex)}")

    try:
      msg.assert_has_keys('_session')
      msg.assert_content(lambda msg: msg.get('_session') in ['start_success', 'start_failure'], "Invalid value for _session")
      if msg.get('_session') == 'start_failure':
        msg.assert_has_keys('_message')

    except RemoteError as ex:
      await self.state.set(InteractiveSessionState.CLOSE)
      raise InteractiveSessionError(f"Malformed reply: {ex}")

    state = msg.get('_session')

    if state == 'start_success':
      self.publisher = DirectPublisher(msg.ch, msg.reply_to, reply_to=self.queue.name)
      await self.state.set(InteractiveSessionState.RUNNING)

    elif state == 'start_failure':
      await self.state.set(InteractiveSessionState.CLOSE)
      raise InteractiveSessionError(f"Session rejected: {msg.content['_message']}")

#
# Server
#

class InteractiveServerSession(InteractiveSessionBase):
  
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
      await self.state.set(InteractiveSessionState.CLOSE)
      return False

    self.publisher = DirectPublisher(msg.ch, msg.reply_to, reply_to=self.queue.name)
   
    try:
      if validator:
        validator(msg)
    except RemoteError as ex:
      await self.publish({'_session': 'start_failure', '_message': str(ex)})
      await self.state.set(InteractiveSessionState.CLOSE)
      return False

    await self.publish({'_session': 'start_success'})
    await self.state.set(InteractiveSessionState.RUNNING)
    return msg
