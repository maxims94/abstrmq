import asyncio
import logging
from abstrmq import *
from abstrmq.rmqnode import *
from abstrmq.pattern import *

log = logging.getLogger(__name__)

class ClientApp(RMQApp):
  def __init__(self):
    super().__init__()
    self._mgr = TaskManager()

  async def run(self):

    self._ch = await self.client.channel()

    self._queue = FutureQueue(self._ch, '')
    await self._queue.declare()
    await self._queue.start_consume()

    try:
      await self._session()
    except asyncio.CancelledError:
      log.debug("Cancelled")
    finally:
      await self._mgr.close()

  async def _session(self):

    session = InteractiveClientSession(self._queue)
    try:
      await session.publish_start({'command': 'count', 'from': 1, 'to': 10, 'sleep': 0.5}, publisher=DirectPublisher(self._ch, 'interactive_session_test'))
    except InteractiveSessionError as ex:
      log.error("Failed to start session")
      log.error(repr(ex))
      return
    
    log.info("Session successfully started")

