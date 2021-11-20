import logging

from .abstract_service import AbstractService
from .task_manager import TaskManager

log = logging.getLogger(__name__)

class ServiceManager:
  def __init__(self):
    self._services = {}

  def register(self, name, service: AbstractService):
    assert name not in self._services
    self._services[name] = service

  def __getattr__(self, key):
    if key in self._services:
      return self._services[key]
    else:
      raise AttributeError(f"Service '{key}' not found")

  async def run(self):
    """
    When a Task running this coroutine is cancelled, the CancelledError is propagated to `await gather` inside the TaskManager. This, in turn, cancels all Tasks and will, in turn, raise a CancelledError. You don't need a `close()`!
    """
    log.info("Run services")

    mgr = TaskManager()

    for k in self._services:
      log.info(f"Start '{k}' service")
      service = self._services[k]
      mgr.create_task(service.run())

    await mgr.gather()

  # Currently not used
  #async def close(self):
  #  await mgr.close()
