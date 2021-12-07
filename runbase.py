import asyncio
import signal
import os

import logging
log = logging.getLogger(__name__)

class RunBase:
  """
  Wrapper for asyncio.run()

  Handles signals
  """

  def __init__(self):
    self._main_task = None

  def start(self):
    #asyncio.run(self._wrapper())
    asyncio.run(self._wrapper(), debug=True)

  async def _wrapper(self):

    loop = asyncio.get_running_loop()

    def on_signal():
      log.debug("Termination signal received")
      loop.add_signal_handler(signal.SIGINT, lambda: None)
      loop.remove_signal_handler(signal.SIGTERM)
      log.debug("Cancel main task")
      assert self._main_task
      self._main_task.cancel()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
      loop.add_signal_handler(sig, on_signal)

    def handle_exception(loop, context):
      log.error(f"Exception handler: {context.get('exception')}")
      loop.default_exception_handler(context)

    loop.set_exception_handler(handle_exception)

    self._main_task = asyncio.create_task(self._main())
    await self._main_task
  
  async def _main(self):
    log.debug("Start main")

    try:
      await self.run()
    except asyncio.CancelledError:
      log.debug("Main cancelled")
    except Exception as ex:
      log.error("Unexpected error")
      log.exception(ex)

    finally:
      log.debug("Cleanup")

      try:
        await self.cleanup()
      except Exception as ex:
        log.error("Unexpected error during cleanup")
        log.exception(ex)

      log.debug("Cleanup finished")
      log.debug("asyncio.run cleanup")

  async def run(self):
    raise NotImplementedError

  async def cleanup(self):
    raise NotImplementedError
