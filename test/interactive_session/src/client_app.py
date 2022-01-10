import asyncio
import logging
import random
from abstrmq import *
from abstrmq.rmqnode import *
from abstrmq.pattern import *
from contextlib import suppress

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

    self._session = InteractiveClientSession(self._queue)
    self._mgr.create_task(self._run_session())

    try:
      await self._session.closed()
    except asyncio.CancelledError:
      log.debug("Cancelled")
      with suppress(asyncio.TimeoutError):
        await self._session.publish_close()
    finally:
      await self._mgr.close()
      self._session.close()

  async def _run_session(self):

    try:
      config = {'command': 'count', 'from': 1, 'to': random.randint(2,10), 'sleep': random.random()*2}
      log.info(f"Config: {config}")
      await self._session.publish_start(config, publisher=DirectPublisher(self._ch, 'interactive_session_test'))
    except asyncio.TimeoutError as ex:
      log.error("Timeout")
      return
    except InteractiveSessionError as ex:
      log.error("Failed to start session")
      log.error(repr(ex))
      return
    
    log.info("Session successfully started")

    while True:
      try:
        msg = await self._session.receive_message()
      except asyncio.CancelledError:
        log.info("receive_message cancelled")
        return

      log.info(f"Received: {msg.content}")
