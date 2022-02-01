import asyncio
import logging
import random
from abstrmq import *
from abstrmq.rmqnode import *
from abstrmq.pattern import *
from contextlib import suppress

log = logging.getLogger(__name__)

import os

class ClientSession(InteractiveClientSession):

  async def run(self):

    try:
      if not bool(os.environ.get('INVALID', 0)):
        config = {'command': 'count', 'from': 1, 'to': random.randint(2,10), 'sleep': random.random()*2}
      else:
        config = {'command': 'count', 'invalid': 1}

      log.info(f"Config: {config}")
      await self.publish_start(config, publisher=DirectPublisher(self.queue._ch, 'interactive_session_test'))
    except asyncio.TimeoutError as ex:
      log.error("Timeout")
      return
    except InteractiveSessionError as ex:
      log.error("Failed to start session")
      log.error(repr(ex))
      return
    
    log.info("Session successfully started")

    try:
      await self.closed()
    except asyncio.CancelledError:
      log.debug("Cancelled")
      await self.publish_close()
    finally:
      # Will also close the receive loop
      await self._mgr.close()

  async def process_message(self, msg):
    log.info(f"Received: {msg}")

class ClientApp(RMQApp):
  def __init__(self):
    super().__init__()
    self._mgr = SessionManager()

  async def run(self):

    self._ch = await self.client.channel()

    self._queue = FutureQueue(self._ch, '')
    await self._queue.declare()
    await self._queue.start_consume()

    session = await self._mgr.new_session(ClientSession(self._queue))

    try:
      await session.closed()
    except asyncio.CancelledError:
      log.debug("Cancelled")
    finally:
      await self._mgr.close()
