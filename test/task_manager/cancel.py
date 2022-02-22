from abstrmq import *

import asyncio

async def coro():
  #raise Exception("test")
  try:
    await asyncio.sleep(10)
  except asyncio.CancelledError:
    print("cancelled")
    raise

async def main():
  mgr = TaskManager()
  mgr.create_task(coro())
  await asyncio.sleep(1)
  await mgr.close()

asyncio.run(main())
