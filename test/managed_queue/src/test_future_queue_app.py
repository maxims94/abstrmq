import asyncio
import logging
from abstrmq import *
from abstrmq.rmqnode import *

log = logging.getLogger(__name__)

class TestFutureQueueApp(RMQApp):
  def __init__(self):
    super().__init__()
    self._mgr = TaskManager()

  async def run(self):

    ch = await self.client.channel()

    # Should raise errors
    #self._queue = FutureQueue(ch, 'test_app', buffer_size=2, mode=FutureQueueMode.STRICT)

    # Should only consume even ids, i.e. 0, 2, 4 etc.
    #self._queue = FutureQueue(ch, 'test_app', buffer_size=2, mode=FutureQueueMode.DROP, on_drop=self.on_drop)

    # First, all are consumed. Starting with id = 5, the odd are dropped
    #self._queue = FutureQueue(ch, 'test_app', buffer_size=2, mode=FutureQueueMode.WAIT, on_drop=self.on_drop)

    # First, all are consumed. Starting with id = 2, only the evens are displayed, all others are dropped
    self._queue = FutureQueue(ch, 'test_app', buffer_size=2, mode=FutureQueueMode.CIRCULAR, on_drop=self.on_drop)

    await self._queue.declare()
    await self._queue.purge()
    await self._queue.start_consume()

    self._pub = DirectPublisher(ch, 'test_app')

    try:
      self._mgr.create_task(self.consumer())
      self._mgr.create_task(self.producer())

      await asyncio.Future()
    except asyncio.CancelledError:
      log.debug("Cancelled")
    finally:
      await self._mgr.close()

  def on_drop(self, message, reason):
    print(f"Dropped: {message} (reason: {reason})")

  async def consumer(self):
    while True:
      msg = await self._queue.receive()
      print(f"Consumed: {msg}")
      await asyncio.sleep(2)

  async def producer(self):
    i = 0 
    while True:
      await self._pub.publish({'type' : 'test', 'i' : i})
      await asyncio.sleep(1)
      i += 1
