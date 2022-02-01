import asyncio
import logging
from abstrmq import *
from abstrmq.rmqnode import *
from abstrmq.pattern import *
from contextlib import suppress

log = logging.getLogger(__name__)

class BasicSession(InteractiveServerSession):

  async def run(self):

    try:
      msg = await self.receive_start({'command': 'count'})

      msg.assert_has_keys('from', 'to', 'sleep')
      if msg.get('from') >= msg.get('to'):
        raise InvalidMessageError("Constraint violated: from < to")

      self._from = msg.get('from')
      self._to = msg.get('to')
      self._sleep = msg.get('sleep')

    except asyncio.CancelledError:
      return
    except Exception as ex:
      log.info("Failed to start session")
      await self.publish_failure(str(ex))
      return
    
    await self.publish_success()

    log.info(f"New session: {msg.content}")

    self._mgr.create_task(self.interval_counter())

    try:
      await self.closed()
    except asyncio.CancelledError:
      log.debug("Cancelled session")
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

  async def process_message(self, msg):
    log.warning(f"Unexpected message to server: {msg}")
    pass

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
      try:
        s = await self._session.new_session(BasicSession(self._queue))
        await s.started()
      except asyncio.CancelledError:
        break
