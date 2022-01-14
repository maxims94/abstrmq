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
    self._register_future = None

  def _set_corr_id(self, corr_id):
    """
    Can only be called once; the corr_id of a session can't change

    Guarantees that the session is registered once a corr_id is available
    """
    assert not self.corr_id
    log.debug("Set corr_id to %s", corr_id)
    self.corr_id = corr_id

    if isinstance(self.queue, ManagedQueue):
      self._register_future = self.queue.register(corr_id = self.corr_id)

  def _generate_corr_id(self):
    return str(uuid.uuid4())

  def short_id(self):
    if self.corr_id:
      return self.corr_id[:8]
    else:
      return "empty"

  async def receive(self, *args, **kwargs):
    assert self._open
    assert self.queue

    init_receive = self.corr_id is None
    needs_register = isinstance(self.queue, ManagedQueue)

    # Need to register a corr_id independent Future for the first message
    if needs_register:
      tmp_register = None
      if self._register_future is None:
        kwargs_fix = kwargs.copy()
        if 'timeout' in kwargs:
          del kwargs_fix['timeout']
        tmp_register = self.queue.register(*args, **kwargs_fix)

    if not self.corr_id:
      return (await self._receive_init(*args, **kwargs)


    try:
      # self.corr_id may be None
      result = await self.queue.receive(corr_id = self.corr_id, *args, **kwargs)
    except asyncio.CancelledError:
      #log.debug("session: cancelled error")
      raise
    except:
      raise
    else:
      log.debug(f'{self.corr_id}, {result.corr_id}')
      if self.corr_id is None:
        if result.corr_id is not None:
          self.corr_id = result.corr_id

        else:
          log.warning("Session received message without corr_id")

      else:
        if result.corr_id is not None:
          assert self.corr_id == result.corr_id, "Received message with wrong corr_id"
      return result
    finally:
      if isinstance(self.queue, ManagedQueue):
        if tmp_register:
          self.queue.deregister(tmp_register)

  async def publish(self, *args, **kwargs):
    assert self._open
    assert self.publisher

    if self.corr_id is None:
      self.generate_corr_id()

      if isinstance(self.queue, ManagedQueue):
        self._register_future = self.queue.register(corr_id = self.corr_id)

    return await self.publisher.publish(*args, corr_id = self.corr_id, **kwargs)

  def close(self):
    """
    This only prevents the start of new communication, not stop currently running communication (this should be handled by the subclass)

    Note that if you use SessionManager, this is invoked automatically after `run` is done! No need to do it manually!
    """

    assert self._open

    log.debug("Close session: %s", self.corr_id)

    if isinstance(self.queue, ManagedQueue) and self._register_future:
      self.queue.deregister(self._register_future)
      self._register_future = None

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
