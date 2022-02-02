from ..future_queue_session import FutureQueueSession
from ..future_queue import FutureQueue
from ..managed_queue import ManagedQueue
from ..publisher import BasicPublisher, DirectPublisher
from ..exceptions import RemoteError
from ..state_condition import StateCondition
from ..task_manager import TaskManager
import asyncio
from aiormq.exceptions import PublishError
from asyncio import Event
from enum import Enum
import logging

log = logging.getLogger(__name__)

"""
Full-duplex communication between client and server
"""

# TODO: Here, we *can* guarantee that it will consume all messages with a specific corr_id, due to the read loop -> include register / deregister for ManagedQueue
# TODO: timeout for receiving messages from the server (subclasses can't define a timeout, they only react if a message arrives)
# TODO: heartbeat; e.g. close the message if the other side hasn't responded in X seconds after a heartbeat request; reset timer on a new message; this must exist for BOTH client and server!! i.e. part of Base; this hsould be ENOUGH for both client and server to establish that the remote is available

class InteractiveSessionError(Exception):
  pass

class InteractiveSessionState(Enum):
  INIT = 'init'
  START = 'start'
  RUNNING = 'running'
  CLOSED = 'closed'

class InteractiveSessionBase(FutureQueueSession):

  HEARTBEAT_INTERVAL = 10
  HEARTBEAT_TIMEOUT = 2

  def __init__(self, reply_queue: FutureQueue):
    super().__init__(reply_queue)
    # This is necessary as we need to wait for messages and `close` concurrently
    # Use this TaskManager instead of creating your own!
    self._mgr = TaskManager()
    self.state = StateCondition(InteractiveSessionState, InteractiveSessionState.INIT)
    self._has_closed_session = False
    self._heartbeat_fut = None

  @property
  def has_closed_session(self):
    """
    Whether or not this node has initiated closing the session
    """
    return self._has_closed_session

  async def publish_message(self, *args, **kwargs):
    """
    In running state, publish a message to the other node

    In a running session, we expect that all messages that are sent will be delivered (i.e. mandatory=True). This is the default. You can switch this off by setting the kwarg explicitly

    :raises: TimeoutError
    """
    assert self.state.is_in(InteractiveSessionState.RUNNING)

    if 'mandatory' not in kwargs:
      kwargs['mandatory'] = True

    try:
      await self.publish(*args, **kwargs)
    except PublishError as ex:
      await self.state.set(InteractiveSessionState.CLOSED)
      raise InteractiveSessionError(f"Can't route message: {repr(ex)}")
  
  async def _base_heartbeat(self):

    assert self.is_started()

    while True:

      if self.is_closed():
        return
      
      try:
        await asyncio.sleep(self.HEARTBEAT_INTERVAL)
      except asyncio.CancelledError:
        return

      if self.is_closed():
        return

      self._heartbeat_fut = asyncio.Future()

      try:
        await self.publish({'_session': 'ping'}, mandatory=True)
      except Exception as ex:
        log.error("Failed to publish heartbeat: {repr(ex)})")
        await self.state.set(InteractiveSessionState.CLOSED)
        return
      except asyncio.CancelledError:
        return

      try:
        await asyncio.wait_for(self._heartbeat_fut, timeout=self.HEARTBEAT_TIMEOUT)
      except asyncio.TimeoutError:
        log.error("Heartbeat timed out")
        await self.state.set(InteractiveSessionState.CLOSED)
        return
      else:
        log.debug("Heartbeat successful")

  async def _publish_pong(self):
    try:
      await self.publish({'_session': 'pong'})
    except asyncio.CancelledError:
      return
    except Exception as ex:
      log.warning(f"Failed to send pong: {ex}")

  async def _base_recv_loop(self):
    """
    Advantages of receive loop:
    * low complexity
    * all internal messages are processed in one place
    * immediately covers all messages with the respective corr_id (for ManagedQueue)
    """

    assert self.is_started()

    while True:

      if self.is_closed():
        log.debug("Session is closed")
        return

      try:
        msg = await self.receive()
      except asyncio.CancelledError:
        log.debug("Cancel receive loop")
        break

      if self.is_closed():
        log.warning(f"Dropped message due to closed session: ({msg.short_str()})")
        return

      if '_session' in msg.content:
        if msg.content == {'_session': 'close'}:
          log.debug("Close received")
          await self.state.set(InteractiveSessionState.CLOSED)
          break

        elif msg.content == {'_session': 'ping'}:
          log.debug("Ping received")
          self._mgr.create_task(self._publish_pong())

        elif msg.content == {'_session': 'pong'}:
          log.debug("Pong received")
          if self._heartbeat_fut is not None and not self._heartbeat_fut.done():
            self._heartbeat_fut.set_result(True)

        else:
          # Ignore invalid message
          log.debug(f"Invalid internal message: {msg.short_str()}")
      else:
        # NOT in a task; ensures that you process messages chronologically
        await self.process_message(msg)

  async def process_message(self, msg):
    """
    Callback function for payload of messages

    msg is a dict, not a QueueMessage!
    """
    pass

  async def publish_close(self):
    """
    Node closes session and notifies the remote node

    This is a courtesy message!

    It should be unproblematic if this fails (e.g. because the channel is closed -- a common reason for cancellation!)

    The remote will eventually realize that this connection is closed because heartbeats will fail

    NOT mandatory; we don't care if the message is dropped
    """
    assert self.is_started()

    if self.is_closed():
      return

    # Do this first since this is used read by `finally`, which is invoked after setting state to CLOSED
    # Set this to True even if publishing the message fails
    self._has_closed_session = True

    # Do this before publishing so that the receive loop will drop any messages received from now on
    # This will typically also start the `finally` clause of `run`
    await self.state.set(InteractiveSessionState.CLOSED)
    
    # Suppress timeout, closed channel, bad connection etc.
    # The application should not fail because of this!
    try:
      await self.publish({'_session': 'close'})
    except asyncio.CancelledError:
      log.warning("publish_close cancelled")
    except Exception as ex:
      log.warning(f"publish_close didn't run: {ex}")

  def started(self):
    """
    Usage:

    await session.started()
    """
    return self.state.wait_for(InteractiveSessionState.RUNNING, InteractiveSessionState.CLOSED)

  def closed(self):
    """
    Usage:
    await session.closed()
    """
    return self.state.wait_for(InteractiveSessionState.CLOSED)

  def is_started(self):
    return self.state.is_in(InteractiveSessionState.RUNNING, InteractiveSessionState.CLOSED)

  def is_closed(self):
    return self.state.is_in(InteractiveSessionState.CLOSED)

#
# Client
#

class InteractiveClientSession(InteractiveSessionBase):

  START_REPLY_TIMEOUT = 2

  async def publish_start(self, body, publisher: BasicPublisher, timeout=None, **kwargs):
    """
    Initiate a session by sending a request to the server. reply_to is added automatically

    :raises: InteractiveSessionError
    """

    assert self.state.is_in(InteractiveSessionState.INIT)

    if timeout is None:
      timeout = self.START_REPLY_TIMEOUT

    self.publisher = publisher
    publisher.reply_to = self.queue.name

    kwargs['mandatory'] = True
    try:
      await self.publish(body, **kwargs)
    except PublishError as ex:
      await self.state.set(InteractiveSessionState.CLOSED)
      raise InteractiveSessionError(f"Can't route publish_start message: {repr(ex)}")

    await self.state.set(InteractiveSessionState.START)

    try:
      msg = await self.receive(timeout=timeout)
    except asyncio.TimeoutError:
      await self.state.set(InteractiveSessionState.CLOSED)
      raise InteractiveSessionError("Timeout")

    try:
      msg.assert_reply_to()
      msg.assert_corr_id()
    except RemoteError as ex:
      await self.state.set(InteractiveSessionState.CLOSED)
      raise InteractiveSessionError(f"Malformed reply: {repr(ex)}")

    try:
      msg.assert_has_keys('_session')
      msg.assert_content(lambda msg: msg.get('_session') in ['start_success', 'start_failure'], "Invalid value for _session")
      if msg.get('_session') == 'start_failure':
        msg.assert_has_keys('_message')

    except RemoteError as ex:
      await self.state.set(InteractiveSessionState.CLOSED)
      raise InteractiveSessionError(f"Malformed reply: {ex}")

    state = msg.get('_session')

    if state == 'start_success':
      self.publisher = DirectPublisher(msg.ch, msg.reply_to, reply_to=self.queue.name)
      await self.state.set(InteractiveSessionState.RUNNING)
      self._mgr.create_task(self._base_recv_loop())
      self._mgr.create_task(self._base_heartbeat())

    elif state == 'start_failure':
      await self.state.set(InteractiveSessionState.CLOSED)
      raise InteractiveSessionError(f"Session rejected: {msg.content['_message']}")

#
# Server
#

class InteractiveServerSession(InteractiveSessionBase):
  """
  Protocol:

  msg = receive_start()
  [process message]
  Then, you must either publish_success() or publish_failure() (and terminate the session)
  """

  async def receive_start(self, *args, **kwargs):
    """
    Wait for the client to initiate a new session.

    :param validator: a callback that checks whether the request is valid. Takes one argument (the message). Expected to throw a RemoteError if the request is invalid.
    :raises: TimeoutError, RemoteError, any Exception from process_message
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
      raise

    self.publisher = DirectPublisher(msg.ch, msg.reply_to, reply_to=self.queue.name)

    return msg

  async def publish_success(self):

    try:
      await self.publish({'_session': 'start_success'}, mandatory=True)
    except PublishError as ex:
      await self.state.set(InteractiveSessionState.CLOSED)
      raise InteractiveSessionError(f"Can't route publish_success message: {repr(ex)}")

    await self.state.set(InteractiveSessionState.RUNNING)
    self._mgr.create_task(self._base_recv_loop())
    self._mgr.create_task(self._base_heartbeat())

  async def publish_failure(self, msg):

    await self.publish({'_session': 'start_failure', '_message': msg}, mandatory=False)
    await self.state.set(InteractiveSessionState.CLOSED)
