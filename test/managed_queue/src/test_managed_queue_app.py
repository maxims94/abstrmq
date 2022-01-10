import asyncio
import logging
from abstrmq import *
from abstrmq.rmqnode import *

log = logging.getLogger(__name__)

class TestManagedQueueApp(RMQApp):
  def __init__(self):
    super().__init__()
    self._mgr = TaskManager()

  async def run(self):

    ch = await self.client.channel()

    self._queue = ManagedQueue(ch, 'test_app', buffer_size=2, on_drop=self.on_drop)

    await self._queue.declare()
    await self._queue.purge()
    await self._queue.start_consume()

    self._pub = DirectPublisher(ch, 'test_app')

    # Test: Don't register anything
    # Expect: All messages are unregistered

    # Test: Register only even
    #self._queue.register(custom=lambda msg: msg.get('i') % 2 == 0)

    # Test: Register only even and multiples of 3
    self._queue.register(custom=lambda msg: msg.get('i') % 2 == 0)
    self._queue.register(custom=lambda msg: msg.get('i') % 3 == 0)

    try:
      self._mgr.create_task(self.consumer())
      self._mgr.create_task(self.producer())

      await asyncio.Future()
    except asyncio.CancelledError:
      log.debug("Cancelled")
    finally:
      await self._mgr.close()

  def on_drop(self, message, reason):
    print(f"Dropped: {message} (reason: {reason.value})")

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
