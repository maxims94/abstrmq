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
  """
  # TODO: match any / all

  def __init__(self, match_dict = {}, corr_id=None, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self._match_dict = match_dict
    self._corr_id = corr_id
    self._result_value = None

  def process(self, msg : QueueMessage):
    if self.done():
      return False

    assert not self.cancelled(), "Can't pass a message to a cancelled MessageFuture"
    assert not self.done(), f"Tried to process {msg} on a MessageFuture that is already set to {self._result_value}"

    if self._match_dict:
      if not msg.match_dict(self._match_dict):
        return False

    if self._corr_id:
      if msg.corr_id != self._corr_id:
        return False

    self.set_result(msg)
    self._result_value = msg
    return True

  def cancel(self, *args, **kwargs):
    #log.debug(f"Cancel {self!r}")
    return super().cancel(*args, **kwargs)
  
  def __str__(self):
    return f"<{self._match_dict!r}, corr_id={self._corr_id}>"
  
  def __repr__(self):
    return str(self)
  
class FutureQueueMode(Enum):
  """
  What do we do with messages for which no Future has been found?

  DROP: ignore them / drop them
  STRICT: raise an error
  WAIT: wait (indefinitely) until a matching Future has been passed via receive
  """
  DROP = 'drop'
  STRICT = 'strict'
  WAIT = 'wait'

class FutureQueueException(Exception):
  pass

class FutureQueue(BasicQueue):
  # TODO: wait; ensure that the task is interrupted after receive timeout

  # TODO: make sure that there is no buffer overflow; timeout for messages? function to drop all waiting messages? better "mode"?
  """

  :param mode: Specifies behavior for unexpected messages, either a FutureQueueMode or a function that takes the unexpected message and returns a FutureQueueMode
  """
  
  # Dev: make this very small to notice failures quickly
  WAIT_QUEUE_SIZE = 5

  def __init__(self, *args, mode : FutureQueueMode = FutureQueueMode.DROP, **kwargs):
    super().__init__(*args, **kwargs)
    self._receive = []
    self._wait = []
    self._mode = mode

  async def _process_message(self, message):

    #def print_error(fut):
    #  if fut.done() and fut.exception():
    #    log.error(f"Error in _process_message: {fut.exception()}")
    #asyncio.current_task().add_done_callback(print_error)

    log.debug(f"Process message {message!r}")
    
    found = False
    n = 0
    for f in self._receive:
      if f.process(message):
        found = True
        n += 1

    if found:
      log.debug(f"{n} Future(s) found for {message!r}")

    if not found:
      log.debug(f"No future found for {message!r}")

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
        log.debug(f"Add to wait queue: {message!s}")
        self._wait.append(message) 
        assert len(self._wait) <= self.WAIT_QUEUE_SIZE
      if selected_mode is FutureQueueMode.DROP:
        log.warning(f"Drop message: {message!s}")

  async def start_consume(self, **kwargs):
    await super().start_consume(self._process_message, **kwargs)

  def receive_future(self, *args, timeout=None, **kwargs):
    """
    Works like receive, but returns a future
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
      try:
        if fut.cancelled():
          pass
          #log.debug(f"on_done:MessageFuture cancelled: {fut!r}")
      except Exception as ex:
        # Send to global default exception handler
        raise ex
        #log.exception(ex)
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

    #if future._match_dict != {'service':'counter'}:
    #  self._receive.append(future)
    self._receive.append(future)

    # Remove a MessageFuture from the list when it is done
    # Removes if when a result is set
    # Ensures that a cancelled future is removed
    # This can happen if the user decides they no longer need a message
    def on_done(fut):
      if fut.cancelled():
        log.debug(f"on_done:MessageFuture cancelled: {fut!r}")
      #if future._match_dict != {'service':'counter'}:
      #  self._receive.remove(fut)
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
