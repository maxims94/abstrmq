import asyncio
import logging
from abstrmq import *
from abstrmq.rmqnode import *
from abstrmq.pattern import *
from contextlib import suppress

log = logging.getLogger(__name__)

class BasicSession(InteractiveServerSession):

  async def run(self):

    def validate(msg):
      msg.assert_has_keys('from', 'to', 'sleep')
      if msg.get('from') >= msg.get('to'):
        raise InvalidMessageError("Constraint violated: from < to")

    msg = await self.receive_start({'command': 'count'}, validator=validate)

    if not msg:
      log.info("Failed to start session")
      return

    log.info(f"New session: {msg.content}")

    self._from = msg.get('from')
    self._to = msg.get('to')
    self._sleep = msg.get('sleep')

    self._mgr.create_task(self.interval_counter())

    try:
      await self.closed()
    except asyncio.CancelledError:
      log.debug("Cancelled session")
      with suppress(asyncio.TimeoutError):
        await self.publish_close()
    finally:
      await self._mgr.close()

  async def interval_counter(self):
    i = self._from
    while i <= self._to:
      log.info(f"Publish: {i}")
      with suppress(asyncio.TimeoutError):
        await self.publish_message({'current': i})

      await asyncio.sleep(self._sleep)
      i += 1

    await self.publish_close()

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
      log.debug("Cancelled app")
    finally:
      await self._session.close()
      await self._mgr.close()

  async def session_loop(self):
    while True:
      s = await self._session.new_session(BasicSession(self._queue))
      await s.started()
