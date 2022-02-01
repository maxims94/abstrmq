import aiormq
import asyncio
import logging
from signal import SIGINT, SIGTERM

log = logging.getLogger(__name__)

class RMQClient:

  def __init__(self):
    self._conn = None
    self._ch = []
    self.on_close = None
    self.closed = False

  async def connect(self, *args, **kwargs):
    """
    Raises ConnectionError
    """
    log.debug(f"Connect to {args!r}")
    self._conn = await aiormq.connect(*args, **kwargs)

    self._conn.closing.add_done_callback(self.on_connection_close)

  def on_connection_close(self, fut):
    log.debug("Connection closed")

    self.closed = True

    if self.on_close:
      self.on_close()

  def on_channel_close(self, fut):
    log.debug("Channel closed")

    self.closed = True

    if self.on_close:
      self.on_close()

  async def channel(self):
    """
    Raises Exception
    """
    assert self._conn
    log.debug("New channel")
    ch = await self._conn.channel()
    self._ch.append(ch)

    ch.closing.add_done_callback(self.on_channel_close)

    return ch

  async def close(self):
    log.debug("Close")
    self.closed = True

    try:
      for ch in self._ch:
        await ch.close()

      if self._conn:
        await self._conn.close()
        # Hack: Wait until aiormq's auxiliary tasks are done (they are not awaited for some reason)
        await asyncio.sleep(0.1)

    except Exception as ex:
      log.debug(f"Exception while closing: {ex}")
