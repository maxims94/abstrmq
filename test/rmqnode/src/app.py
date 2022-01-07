import asyncio
import logging
from abstrmq.rmqnode import RMQApp

log = logging.getLogger(__name__)

class stBaseApp(RMQApp):
  async def run(self):
    ch = await self.client.channel()
    try:
      await asyncio.sleep(1000)
    except asyncio.CancelledError:
      log.info("Cancelled")
      #self.exit_code = 1
    else:
      pass
      #self.exit_code = 0 

