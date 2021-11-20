import uuid
import logging
import asyncio

from .future_queue import FutureQueue
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

  def generate_corr_id(self):
    """
    Generates an corr_id when requested

    Why not ensure_corr_id? Because his class may be used in a server; in this case, it is assigned a corr_id!
    """
    assert not self.corr_id
    self.corr_id = str(uuid.uuid4())
    log.debug("Generate corr_id: %s", self.corr_id)

  def short_id(self):
    if self.corr_id:
      return self.corr_id[:8]
    else:
      return "empty"

  async def receive(self, *args, **kwargs):
    assert self._open
    assert self.queue

    #def on_done(fut):
    #  if fut.done() and fut.exception():
    #    log.critical(fut.exception())
    #asyncio.current_task().add_done_callback(on_done)

    try:
      # self.corr_id may be None
      result = await self.queue.receive(corr_id = self.corr_id, *args, **kwargs)
    except asyncio.CancelledError:
      #log.debug("session: cancelled error")
      raise
    except:
      raise
    else:
      if self.corr_id is None:
        if result.corr_id is not None:
          self.corr_id = result.corr_id
          log.debug("Set corr_id to %s", self.corr_id)
        else:
          log.warning("Session received message without corr_id")
      return result

  async def publish(self, *args, **kwargs):
    assert self._open
    assert self.publisher

    if not self.corr_id:
      self.generate_corr_id()
    return await self.publisher.publish(*args, corr_id = self.corr_id, **kwargs)

  def close(self):
    """
    This only prevents the start of new communication, not stop currently running communication (this should be handled by the subclass)
    """

    assert self._open

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
