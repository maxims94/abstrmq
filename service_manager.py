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

  def get(self, key):
    if key in self._services:
      return self._services[key]
    else:
      raise AttributeError(f"Service '{key}' not found")

  async def run(self):
    """
    You don't need a `close()`: When a Task running this coroutine is cancelled, this cancels `await gather`. This cancels all Tasks due to the internal logic of `gather`.
    """
    log.info("Run services")

    # Use an instance so that we can reset it if necessary
    # TODO: Maybe this is unnecessary and we should subclass from TaskManager instead
    self._mgr = TaskManager()

    for k in self._services:
      log.info(f"Start '{k}' service")
      service = self._services[k]
      self._mgr.create_task(service.run())

    await self._mgr.gather()

  def cancel(self):
    self._mgr.cancel()

  async def close(self):
    await self._mgr.close()
