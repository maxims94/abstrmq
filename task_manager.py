import asyncio
import logging
from enum import Enum
import pdb

log = logging.getLogger(__name__)

# TODO
# Support task naming and getting a task by name
# asyncio.create_task(coro, *, name=None)!! as well as Task.name etc.!

"""
TaskManager manages a collection of tasks

Note that as long as you ensure that the tasks exit, you don't necessarily need to do `await tmgr.close()` or similar! The cleanup is done automatically!
"""

class TaskManager:
  def __init__(self):
    self._tasks = []
    self.callback_exception = True
    self.gather_exception = False

  def create_task(self, coro):
    t = asyncio.create_task(coro)
    t.add_done_callback(self._on_done)
    self._tasks.append(t)
    return t

  def _on_done(self, future):
    #log.debug("on_done")
    self._tasks.remove(future)
    
    if self.callback_exception:
      if not future.cancelled():
        if future.exception():
          #pdb.set_trace()
          log.debug("Exception in on_done")
          # This will raise an exception
          # It will be passed to the global default exception handler since it is an exception raised in a callback function
          future.result()

  def cancel(self):
    """
    Only send cancel signal
    """
    for t in self._tasks:
      #log.debug("cancel")
      t.cancel()

  async def close(self):
    """
    Send cancel signal and wait
    """
    self.cancel()
    await self.gather()

  async def gather(self):
    # The callback will remove the tasks from the list when they are done
    # So, make a copy
    ts = self._tasks[:]

    result = await asyncio.gather(*self._tasks, return_exceptions=True)

    if self.gather_exception:
      for t in ts:
        # We ignore it if a Task was cancelled (unhandled CancelledError)
        # We only raise an exception if case there was a genuine exception (not CancelledError)
        # See test/task_manager/cancel.py
        if not t.cancelled() and t.exception():
          log.debug(f"Exception in {t}")
          t.result()
