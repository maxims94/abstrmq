import asyncio
import signal
import os
import sys

import logging
log = logging.getLogger(__name__)

class RunBase:
  """
  Wrapper for asyncio.run()

  Handles signals
  """

  def __init__(self):
    """
    :param global_exception_handler: a callable that takes an exc object. If it returns False, the default exception handler of RunBase will be suppressed
    """
    self.main_task = None
    self.cancellable = True
    self.on_done = None
    self.global_exception_handler = None

  def start(self):
    #asyncio.run(self._wrapper())
    asyncio.run(self._wrapper(), debug=True)
    log.debug("Loop closed")
    if self.on_done:
      self.on_done()

  async def _wrapper(self):

    loop = asyncio.get_running_loop()

    def on_signal():
      log.debug("Termination signal received")
      loop.add_signal_handler(signal.SIGINT, lambda: None)
      loop.remove_signal_handler(signal.SIGTERM)
      if self.cancellable:
        log.debug("Cancel main task")
        assert self.main_task
        self.main_task.cancel()
        self.cancellable = False
    
    for sig in (signal.SIGINT, signal.SIGTERM):
      loop.add_signal_handler(sig, on_signal)

    def handle_exception(loop, context):
      exc = context.get('exception')
      if self.global_exception_handler:
        if self.global_exception_handler(exc) is False:
          return
      log.error(f"Global exception handler: {context.get('exception')}")
      loop.default_exception_handler(context)

    loop.set_exception_handler(handle_exception)

    self.main_task = asyncio.create_task(self.main())
    await self.main_task
  
  async def main(self):
    """
    Can be overriden by subclass
    """
    log.debug("Start main")

    try:
      await self.run()
    except asyncio.CancelledError:
      log.debug("Main cancelled")
    except Exception as ex:
      log.error("Unexpected error")
      log.exception(ex)

    finally:
      self.cancellable = False
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
