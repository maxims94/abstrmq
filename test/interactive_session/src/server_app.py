import asyncio
import logging
from abstrmq import *
from abstrmq.rmqnode import *
from abstrmq.pattern import *

log = logging.getLogger(__name__)

class BasicSession(InteractiveServerSession):

  async def run(self):
    msg = await self.receive_start({'command': 'count'})
    log.info(msg)

class ServerApp(RMQApp):
  def __init__(self):
    super().__init__()
    self._mgr = TaskManager()
    self._session = SessionManager()

  async def run(self):

    ch = await self.client.channel()

    self._queue = FutureQueue(ch, 'interactive_session_test')

    await self._queue.declare()
    await self._queue.purge()
    await self._queue.start_consume()

    self._mgr.create_task(self.session_loop())

    try:
      await asyncio.Future()
    except asyncio.CancelledError:
      log.debug("Cancelled")
    finally:
      await self._mgr.close()

  async def session_loop(self):
    while True:
      s = await self._session.new_session(BasicSession(self._queue))
      await s.started()
