import asyncio
import aiormq
import logging
from enum import Enum

from .basic_queue import BasicQueue, QueueMessage

log = logging.getLogger(__name__)

class MessageFuture(asyncio.Future):
  """
  A Future that represents criteria for a message

  It will be set once a message is passed to it that satisfies the criteria

  :param custom: a callable that takes a message and returns True or False
  """
  # TODO: match any / all

  def __init__(self, match = {}, headers = {}, headers_eq = {}, corr_id=None, custom=None, *args, **kwargs):
    super().__init__(*args, **kwargs)

    assert isinstance(match, dict)
    assert isinstance(headers, dict)
    assert isinstance(headers_eq, dict)
    assert custom is None or callable(custom)

    self._match = match
    self._headers = headers
    self._headers_eq = headers_eq
    self._corr_id = corr_id
    self._custom = custom
    self._result_value = None

  def is_match(self, msg : QueueMessage):
    if self._match:
      if isinstance(msg.content, dict):
        if not msg.match_dict(self._match):
          return False

    if self._headers:
      if not msg.match_headers_dict(self._headers):
        return False

    if self._headers_eq:
      if msg.headers != self._headers_eq:
        return False

    if self._corr_id:
      if msg.corr_id != self._corr_id:
        return False

    if self._custom:
      if not self._custom(msg):
        return False

    return True

  def process(self, msg : QueueMessage):
    if self.done():
      return False

    assert not self.cancelled(), "Can't pass a message to a cancelled MessageFuture"
    assert not self.done(), f"Tried to process {msg} on a MessageFuture that is already set to {self._result_value}"
    
    if self.is_match(msg):
      self.set_result(msg)
      self._result_value = msg
      return True
    else:
      return False

  def cancel(self, *args, **kwargs):
    #log.debug(f"Cancel {self!r}")
    return super().cancel(*args, **kwargs)
  
  def __str__(self):
    tmp = {
        'corr_id' : self._corr_id,
        'headers_match' : self._headers,
        'headers_eq': self._headers_eq
    }
    attr = [repr(self._match)] + [f"{k}={tmp[k]}" for k in sorted(tmp.keys()) if tmp[k]]
    return f"<{', '.join(attr)}>"
  
  def __repr__(self):
    return str(self)
  
class FutureQueueMode(Enum):
  """
  What do we do with messages for which no Future has been found?

  DROP: drop unmatched messages
  STRICT: raise an error if a message can't be matched
  WAIT: wait (indefinitely) until a matching Future has been passed via receive; if the buffer is full, messages will be dropped
  CIRCULAR: Like WAIT, only that old messages are dropped if the buffer is full and they haven't been used (buffer overflow safe!)
  """
  DROP = 'drop'
  STRICT = 'strict'
  WAIT = 'wait'
  CIRCULAR = 'circular'

class FutureQueueDropReason(Enum):
  """
  Represents the reason why a message was dropped. Passed to on_drop callback

  NO_FUTURE_FOUND: If a matching Future has not been found (for DROP)
  BUFFER_FULL: If the buffer is full (WAIT, CIRCULAR)
  NOT_REGISTERED: The message has not been registered (used by ManagedQueue)
  """
  NO_FUTURE_FOUND = 'no_future_found'
  FULL_BUFFER = 'full_buffer'
  NOT_REGISTERED = 'not_registered'

class FutureQueueException(Exception):
  pass

class FutureQueue(BasicQueue):
  # TODO: use Queue instead of linked list
  """

  :param mode: Specifies behavior for unexpected messages, either a FutureQueueMode or a function that takes the unexpected message and returns a FutureQueueMode
  :param on_drop: a callable of the form func(msg: QueueMessage, reason: FutureQueueDropReason). It is called every time a message is dropped in the DROP, WAIT and CIRCULAR modes
  """
  
  def __init__(self, *args, mode : FutureQueueMode = FutureQueueMode.DROP, on_drop = None, buffer_size = 1000, **kwargs):
    super().__init__(*args, **kwargs)
    self._receive = []
    self._wait = []
    self._mode = mode
    self._on_drop = on_drop or self._on_drop_default
    self._buffer_size = buffer_size

  async def _process_message(self, message):

    log.debug(f"Process message {message}")
    
    found = False
    n = 0
    for f in self._receive:
      if f.process(message):
        found = True
        n += 1

    if found:
      log.debug(f"{n} Future(s) found for {message}")

    if not found:
      log.debug(f"No future found for {message}")

      selected_mode = None

      if isinstance(self._mode, FutureQueueMode):
        selected_mode = self._mode
      elif callable(self._mode):
        selected_mode = self._mode(message)
        assert isinstance(selected_mode, FutureQueueMode)
      else:
        assert False

      if selected_mode is FutureQueueMode.STRICT:
        raise FutureQueueException("Couldn't find a matching future for a message")

      if selected_mode is FutureQueueMode.WAIT:
        if len(self._wait) < self._buffer_size:
          log.debug(f"Add to queue: {message}")
          self._wait.append(message) 
        else:
          log.warning(f"Message buffer full: Drop message: {message!r}")
          self._on_drop(message, FutureQueueDropReason.FULL_BUFFER)

      if selected_mode is FutureQueueMode.DROP:
        log.warning(f"Drop message: {message!r}")
        self._on_drop(message, FutureQueueDropReason.NO_FUTURE_FOUND)

      if selected_mode is FutureQueueMode.CIRCULAR:
        # At no point are two messages added, so == is enough
        if len(self._wait) == self._buffer_size:
          old_message = self._wait.pop(0)
          log.warning(f"Dropped old message: {old_message!r}")
          self._on_drop(old_message, FutureQueueDropReason.FULL_BUFFER)

          # Sanity check (could fail if you have multiple threads)
          assert len(self._wait) < self._buffer_size

        log.debug(f"Add to queue: {message}")
        self._wait.append(message) 

  async def start_consume(self, **kwargs):
    await super().start_consume(self._process_message, **kwargs)

  def receive_future(self, *args, timeout=None, **kwargs):
    """
    Works like receive, but returns a future
    TODO: Not tested
    """

    future = MessageFuture(*args, **kwargs)
    log.debug('Wait for: %s', future)

    # Try to find a message in the waiting queue first
    # If we find a suitable one, we don't need the done callback since we processed it immediately and didn't add it to `_receive`
    for message in self._wait:
      if future.process(message):
        log.debug(f"Future found for: {message!r}")
        self._wait.remove(message)
        return future

    self._receive.append(future)

    # Remove a MessageFuture from the list when it is done
    # Removes if when a result is set
    # Ensures that a cancelled future is removed
    # This can happen if the user decides they no longer need a message
    def on_done(fut):
      if fut.cancelled():
        log.debug(f"on_done:MessageFuture cancelled: {fut!r}")
      self._receive.remove(fut)

    future.add_done_callback(on_done)

    if not timeout:
      return future
    else:
      return asyncio.create_task(asyncio.wait_for(future, timeout=timeout))

  async def receive(self, *args, timeout=None, **kwargs):
    """
    Raises asyncio.TimeoutError if a timeout was set and elapsed before the message arrived
    """
    
    future = MessageFuture(*args, **kwargs)
    log.debug('Wait for: %s', future)

    # Try to find a message in the waiting queue first
    # If we find a suitable one, we don't need the done callback since we processed it immediately and didn't add it to `_receive`
    for message in self._wait:
      if future.process(message):
        log.debug(f"Future found for: {message!r}")
        self._wait.remove(message)
        return future.result()

    self._receive.append(future)

    # Remove a MessageFuture from the list when it is done
    # Removes if when a result is set
    # Ensures that a cancelled future is removed
    # This can happen if the user decides they no longer need a message
    def on_done(fut):
      if fut.cancelled():
        log.debug(f"on_done:MessageFuture cancelled: {fut!r}")
      self._receive.remove(fut)

    future.add_done_callback(on_done)

    if not timeout:
      return await future
    else:
      try:
        return await asyncio.wait_for(future, timeout=timeout)
      except asyncio.CancelledError:
        raise
      except asyncio.TimeoutError as ex:
        log.debug(f'Message timeout: {future}')
        raise

    # TODO: Alternative is to have a normal routine that returns a Future
    # if not timeout:
    #   return future
    # else:
    #   t = asyncio.create_task(asyncio.wait_for(future, timeout=timeout))
    #   t.add_done_callback(react_to_timeout)
    #   return t

  def _on_drop_default(self, msg, reason):
    if reason is FutureQueueDropReason.FULL_BUFFER:
      log.warning(f"Buffer full. Drop: {msg.short_str()}")
    elif reason is FutureQueueDropReason.NOT_REGISTERED:
      log.warning(f"Unexpected message. Drop: {msg.short_str()}")
    elif reason is FutureQueueDropReason.NO_FUTURE_FOUND:
      log.warning(f"No future found. Drop: {msg.short_str()}")
