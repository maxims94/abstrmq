import asyncio
import logging
from abstrmq import *
from abstrmq.rmqnode import *

log = logging.getLogger(__name__)

class ClientApp(RMQApp):
  def __init__(self):
    super().__init__()
    self._mgr = TaskManager()

  async def run(self):

    ch = await self.client.channel()

    self._queue = FutureQueue(ch, 'interactive_session_test')

    await self._queue.declare()
    await self._queue.purge()
    await self._queue.start_consume()

    try:

      await asyncio.Future()
    except asyncio.CancelledError:
      log.debug("Cancelled")
    finally:
      await self._mgr.close()
