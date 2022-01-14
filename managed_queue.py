from .future_queue import *
import logging

log = logging.getLogger(__name__)

class ManagedQueue(FutureQueue):
  """
  Unregistered messages will be dropped immediately. on_drop is called with NOT_REGISTERED
  """
  def __init__(self, *args, **kwargs):
    kwargs['mode'] = FutureQueueMode.WAIT
    super().__init__(*args, **kwargs)
    self._register = []

  def register(self, *args, **kwargs) -> MessageFuture:
    fut = MessageFuture(*args, **kwargs)
    self._register.append(fut)
    log.debug(f"Register: {fut!r}")
    return fut

  def deregister(self, fut : MessageFuture):
    assert fut in self._register
    self._register.remove(fut)
    log.debug(f"Deregister: {fut!r}")

  async def _process_message(self, message):
    if any(fut.is_match(message) for fut in self._register):
      await super()._process_message(message)
    else:
      log.debug(f"Unregistered message found: {message.short_str()}")
      self._on_drop(message, FutureQueueDropReason.NOT_REGISTERED)
