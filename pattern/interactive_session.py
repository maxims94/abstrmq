from ..future_queue_session import FutureQueueSession
from ..future_queue import FutureQueue
from ..managed_queue import ManagedQueue
from ..publisher import BasicPublisher, DirectPublisher, HeadersExchangePublisher
from ..basic_exchange import HeadersExchange
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

  HEARTBEAT_ENABLED = True
  HEARTBEAT_INTERVAL = 10
  HEARTBEAT_TIMEOUT = 2

  def __init__(self, reply_queue: FutureQueue):
    super().__init__(reply_queue)
    # This is necessary as we need to wait for messages and `close` concurrently
    # Use this TaskManager instead of creating your own!
    self._mgr = TaskManager()
    self.state = StateCondition(InteractiveSessionState, InteractiveSessionState.INIT)
    self._has_closed_session = False
    self._has_remote_closed = False
    self._heartbeat_fut = None
    self._heartbeat_failed = False
    self._register_fut = None
    self._close_message = None

  @property
  def has_closed_session(self):
    """
    Whether or not this node has initiated closing the session
    """
    return self._has_closed_session

  @property
  def has_remote_closed(self):
    return self._has_remote_closed

  @property
  def heartbeat_failed(self):
    return self._heartbeat_failed

  @property
  def close_message(self):
    assert self.is_closed()

    return self._close_message

  @property
  def close_reason(self):
    assert self.is_closed()

    reason = "Unknown reason"
    if self.has_closed_session:
      reason = "Closed by local"
    elif self.heartbeat_failed:
      reason = "Heartbeat failed"
    elif self.has_remote_closed:
      reason = "Closed by remote"

    return reason

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

    if not self.HEARTBEAT_ENABLED:
      return

    log.debug("Start heartbeat")

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
        log.error(f"Failed to publish heartbeat: {repr(ex)})")
        await self.state.set(InteractiveSessionState.CLOSED)
        return
      except asyncio.CancelledError:
        return

      try:
        await asyncio.wait_for(self._heartbeat_fut, timeout=self.HEARTBEAT_TIMEOUT)
      except asyncio.TimeoutError:
        if self.is_closed():
          return
        log.error("Heartbeat timed out")
        self._heartbeat_failed = True
        await self.state.set(InteractiveSessionState.CLOSED)
        return
      else:
        log.info("Heartbeat successful")

  async def _publish_pong(self):
    try:
      await self.publish({'_session': 'pong'}, mandatory=True)
    except asyncio.CancelledError:
      return
    except Exception as ex:
      log.warning(f"Failed to send pong: {ex}")

  async def _deregister_on_close(self):
    try:
      await self.closed()
      if self._register_fut is not None:
        self.deregister(self._register_fut)
    except asyncio.CancelledError:
      return

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
        return

      if self.is_closed():
        log.warning(f"Dropped message due to closed session: ({msg.short_str()})")
        return

      if '_session' in msg.content:
        if msg.get('_session') == 'close':
          message = msg.content.get('_message')
          if message is None:
            message = "No message"
          self._close_message = message
          log.debug("Close received: {message}")

          self._has_remote_closed = True
          try:
            await self.state.set(InteractiveSessionState.CLOSED)
          except asyncio.CancelledError:
            pass
          return

        elif msg.content == {'_session': 'ping'}:
          if self.HEARTBEAT_ENABLED:
            log.debug("Ping received")
            self._mgr.create_task(self._publish_pong())
          else:
            log.debug("Ignore heartbeat")

        elif msg.content == {'_session': 'pong'}:
          if self.HEARTBEAT_ENABLED:
            log.debug("Pong received")
            if self._heartbeat_fut is not None and not self._heartbeat_fut.done():
              self._heartbeat_fut.set_result(True)
          else:
            log.debug("Ignore heartbeat")

        else:
          # Ignore invalid message
          log.debug(f"Invalid internal message: {msg.short_str()}")
      else:
        try:
          # Do NOT run in a task
          # This ensures that messages are processed chronologically
          await self.process_message(msg)
        except Exception as ex:
          # If there are errors during processing, see this as unrecoverable error and end the session
          log.error(f"Error while processing message: {repr(ex)}")
          # TODO: test
          await self.publish_close()
          return
        except asyncio.CancelledError:
          return

  async def process_message(self, msg):
    """
    Callback function for payload of messages

    :param msg: This is a QueueMessage, NOT a dict!
    """
    pass

  async def publish_close(self, message=None):
    """
    Node closes session and notifies the remote node

    This is a courtesy message!

    It should be unproblematic if this fails (e.g. because the channel is closed -- a common reason for cancellation!)

    The remote will eventually realize that this connection is closed because heartbeats will fail

    Use mandatory to prevent trying to publish to a queue that no longer exists!

    Does NOT raise an exception (since it is always handled in the same way: ignoring it)

    Why not publish close_success and close_failure? Since we don't want to build logic that assumes that the close message can be delivered! This is merely a courtesy message!
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
    
    # Suppress timeout, closed channel, bad connection, PublishError etc.
    # The application should not fail because of publish_close!
    try:
      await self.publish({'_session': 'close', '_message': message}, mandatory=True)
    except asyncio.CancelledError:
      log.warning("publish_close cancelled")
    except PublishError as ex:
      log.warning(f"Can't route publish_close message: {ex!r}")
    except Exception as ex:
      log.warning(f"publish_close failed: {ex!r}")

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

  def is_running(self):
    return self.state.is_in(InteractiveSessionState.RUNNING)

  is_closable = is_running

  def is_started(self):
    return self.state.is_in(InteractiveSessionState.RUNNING, InteractiveSessionState.CLOSED)

  def is_closed(self):
    return self.state.is_in(InteractiveSessionState.CLOSED)

#
# Client
#

class InteractiveClientSession(InteractiveSessionBase):

  START_REPLY_TIMEOUT = 2

  async def publish_start(self, body, publisher: BasicPublisher=None, exchange=None, timeout=None, **kwargs):
    """
    Initiate a session by sending a request to the server. reply_to is added automatically

    :raises: InteractiveSessionError

    Either provide a publisher or an exchange
    """

    assert self.state.is_in(InteractiveSessionState.INIT)

    if isinstance(self.queue, ManagedQueue):
      if self.corr_id is None:
        self.set_corr_id()
      self._register_fut = self.register()
      self._mgr.create_task(self._deregister_on_close())

    if timeout is None:
      timeout = self.START_REPLY_TIMEOUT

    if exchange is not None:
      ex = HeadersExchange(self.ch, exchange)

      try:
        await ex.declare()
      except Exception as ex:
        await self.state.set(InteractiveSessionState.CLOSED)
        raise InteractiveSessionError(f"Can't declare exchange: {repr(ex)}")

      self.publisher = HeadersExchangePublisher(self.ch, exchange)
    
    if publisher is not None:
      self.publisher = publisher

    assert self.publisher is not None

    self.publisher.reply_to = self.queue.name

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
  Usage:

  def run():
    msg = receive_start()
    [process message]
    Then, you must either publish_success() or publish_failure() (and terminate the session)

  Session loop:

  async def sma_cross_service(self):
    match_dict = {'class': 'platform', 'method': 'sma_cross'}

    await self._ex.bind(self._queue.name, match_dict)
    fut = self._queue.register(headers_eq=match_dict)

    while True:

      try:
        session = await self._session.new_session(SMACrossSession(self._queue))
        await session.started()
      except asyncio.CancelledError:
        self._queue.deregister(fut)
        break
  """

  async def receive_start(self, *args, **kwargs):
    """
    Wait for the client to initiate a new session.

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

    if isinstance(self.queue, ManagedQueue):
      # corr_id is already set
      self._register_fut = self.register()
      self._mgr.create_task(self._deregister_on_close())

    try:
      await self.publish({'_session': 'start_success'}, mandatory=True)
    except asyncio.TimeoutError:
      await self.state.set(InteractiveSessionState.CLOSED)
      raise InteractiveSessionError(f"Timeout during publish_success")
    except PublishError as ex:
      await self.state.set(InteractiveSessionState.CLOSED)
      raise InteractiveSessionError(f"Can't route publish_success message: {repr(ex)}")
    else:
      await self.state.set(InteractiveSessionState.RUNNING)
      self._mgr.create_task(self._base_recv_loop())
      self._mgr.create_task(self._base_heartbeat())

  async def publish_failure(self, msg):

    try:
      await self.publish({'_session': 'start_failure', '_message': msg}, mandatory=False)
    except Exception as ex:
      log.warning(f"publish_failure failed: {ex}")
    finally:
      await self.state.set(InteractiveSessionState.CLOSED)
