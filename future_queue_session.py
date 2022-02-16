import uuid
import logging
import asyncio

from .future_queue import FutureQueue
from .managed_queue import ManagedQueue
from .publisher import BasicPublisher

log = logging.getLogger(__name__)

class FutureQueueSession:
  """
  Lightweight class that manages corr_id

  It does NOT manage reply_to, which is handled by BasicPublisher
  """
  def __init__(self, queue: FutureQueue = None, publisher: BasicPublisher = None, corr_id: str = None):
    self.queue = queue
    self.publisher = publisher
    self.corr_id = corr_id
    self._open = True
    self.on_close = None
    self.log = None

  def set_corr_id(self, corr_id=None):
    """
    Can only be called once; the corr_id of a session can't change

    This CAN be called by the user if they need the corr_id early. It only makes sense for clients.
    """
    assert self.corr_id is None, "corr_id is already set"

    if corr_id is None:
      corr_id = str(uuid.uuid4())

    log.debug("Set corr_id to %s", corr_id)
    self.corr_id = corr_id

  def short_id(self):
    if self.corr_id:
      return self.corr_id[:8]
    else:
      return "empty"

  async def receive(self, *args, **kwargs):
    assert self._open, "Session not open"
    assert self.queue, "No queue"

    try:
      # self.corr_id may be None
      result = await self.queue.receive(corr_id = self.corr_id, *args, **kwargs)
    except asyncio.CancelledError:
      #log.debug("session: cancelled error")
      raise
    except:
      raise
    else:
      if result.corr_id is not None:
        if self.corr_id is None:
          self.set_corr_id(result.corr_id)
        else:
          if self.corr_id != result.corr_id:
            log.warning("Received message with wrong corr_id")
      else:
        log.warning("Received message without corr_id")
            
      return result

  async def publish(self, *args, **kwargs):
    assert self._open, "Session not open"
    assert self.publisher, "No publisher"

    if self.corr_id is None:
      self.set_corr_id()

    return await self.publisher.publish(*args, corr_id = self.corr_id, **kwargs)

  def close(self):
    """
    This only prevents the start of new communication, not stop currently running communication (this should be handled by the subclass)

    Note that if you use SessionManager, this is invoked automatically after `run` is done! No need to do it manually!
    """

    assert self._open, "Session not open"

    log.debug("Close session: %s", self.corr_id)

    # TODO: on_close can be a coro
    # TODO: remove callback?
    if self.on_close:
      self.on_close(self)

    # This has to come at the end in case on_close needs networking
    self._open = False

  #
  # Logging functions for subclasses
  #

  # TODO: Use LoggerAdapter

  def info(self, msg):
    assert self.log
    self.log.info(msg, extra={'id': self.short_id()})

  def debug(self, msg):
    assert self.log
    self.log.debug(msg, extra={'id': self.short_id()})

  def warning(self, msg):
    assert self.log
    self.log.warning(msg, extra={'id': self.short_id()})

  def error(self, msg):
    assert self.log
    self.log.error(msg, extra={'id': self.short_id()})

  def critical(self, msg):
    assert self.log
    self.log.critical(msg, extra={'id': self.short_id()})

  def __str__(self):
    return str(self.corr_id)

  def __repr__(self):
    return f"<FutureQueueSession corr_id={self.corr_id}>"

  # Remark on ManagedQueues: We can't register a session on all messages with a certain corr_id since we don't know whether it will really handle all of them
  # But we can provide helper methods in case this is true!

  def register(self, *args, **kwargs):
    assert isinstance(self.queue, ManagedQueue), "Must be ManagedQueue"
    assert self.corr_id is not None, "corr_id not set"
    return self.queue.register(corr_id = self.corr_id, *args, **kwargs)

  def deregister(self, fut):
    assert isinstance(self.queue, ManagedQueue), "Must be ManagedQueue"
    self.queue.deregister(fut)
