from .future_queue import *
import logging

log = logging.getLogger(__name__)

class ManagedQueue(FutureQueue):
  """
  Unregistered messages will be dropped immediately.

  on_drop: drop reasons may be NOT_REGISTERED or FULL_BUFFER

  Reasonable on_drop:
  * If reply_to is given, send back a message to the sender to inform them that their request is invalid
  * Merely add an entry to the log, don't send anything back
    * Let the client time out!
    * Avoid the additional server load of sending replies
    * Low complexity, enough for most use cases!

  Example:
  def _on_drop(self, msg, reason):
    if reason is FutureQueueDropReason.FULL_BUFFER:
      log.warning(f"Server overload. Drop: {msg.short_str()}")
    elif reason is FutureQueueDropReason.NOT_REGISTERED:
      log.warning(f"Drop invalid request: {msg.short_str()}")
  """
  def __init__(self, *args, **kwargs):
    kwargs['mode'] = FutureQueueMode.WAIT
    super().__init__(*args, **kwargs)
    self._register = []

  def register(self, *args, **kwargs) -> MessageFuture:
    fut = MessageFuture(*args, **kwargs)

    # TODO: does this work? Or does it miss equal Futures (different object ids)
    assert fut not in self._register, "Future already registered"

    self._register.append(fut)
    log.debug(f"Register: {fut!r}")
    return fut

  def deregister(self, fut : MessageFuture):
    assert fut in self._register, "Future not registered"
    self._register.remove(fut)
    log.debug(f"Deregister: {fut!r}")

  async def _process_message(self, message):
    if any(fut.is_match(message) for fut in self._register):
      await super()._process_message(message)
    else:
      log.debug(f"Unregistered message found: {message.short_str()}")
      self._on_drop(message, FutureQueueDropReason.NOT_REGISTERED)
