import aiormq
import asyncio
import logging
from signal import SIGINT, SIGTERM

log = logging.getLogger(__name__)

class RMQClient:

  def __init__(self):
    self._conn = None
    self._ch = []

  async def connect(self, *args, **kwargs):
    """
    Raises ConnectionError
    """
    log.debug(f"Connect to {args!r}")
    self._conn = await aiormq.connect(*args, **kwargs)

  async def channel(self):
    assert self._conn
    log.debug("Create new channel")
    ch = await self._conn.channel()
    self._ch.append(ch)
    return ch

  async def close(self):
    log.debug("Close")

    for ch in self._ch:
      await ch.close()

    if self._conn:
      await self._conn.close()
