import os
import asyncio
import sys

from .runbase import RunBase
from .rmqapp import RMQApp

from abstrmq import RMQClient

from aiormq import AMQPConnectionError

import logging
log = logging.getLogger(__name__)

class RMQNode(RunBase):
  def __init__(
    self,
    app_cls,
    broker_url = 'amqp://guest:guest@localhost/',
    *,
    restart=False,
    max_conn_tries=1,
    conn_retry_interval=5
  ):
    """
    :param app_cls: class derived from RMQApp
    :param broker_url:
    :param restart: if true, restart the application on connection failure
    :param max_conn_tries: positive integer for limited number, infinite otherwise (use -1 by convention)
    :param retry_interval: time to wait between connection attempts (seconds)
    """
    super().__init__()

    self._app_cls = app_cls
    self._broker_url = broker_url
    self._max_conn_tries = max_conn_tries
    self._conn_retry_interval = conn_retry_interval

    self._restart = restart
    # This only is True if the connection to the broker was interrupted
    # This stays False if the app was terminated by SIGTERM or because run() ended regularly
    # Can be overwritten by app's exit_code
    self._perform_restart = False

    self.on_done = self._on_done

  async def main(self):

    log.debug("Node start")

    self.client = RMQClient()
    num_tries = 0

    while True:
      log.info(f"Try to establish connection (num: {num_tries+1})")

      try:
        await self.client.connect(self._broker_url)
      except asyncio.CancelledError:
        log.debug("Cancelled")
        return
      except ConnectionError as ex:
        log.error(f"Failed to connect to broker: {ex}")
      except AMQPConnectionError as ex:
        # All of these errors are non-recoverable!
        log.error(f"Fatal error: {ex}")
        sys.exit(1)
      except Exception as ex:
        log.error(f"Unexpected error: {ex}")
        sys.exit(1)
      else:
        self.client.on_close = self.on_close
        log.info("Connection established")
        break

      num_tries += 1

      if self._max_conn_tries > 0 and num_tries == self._max_conn_tries:
        log.error("Maximal connection attempts reached")
        # There is no connection to close anyway!
        sys.exit(1)

      log.info(f"Try again in {self._conn_retry_interval}s...")

      try:
        await asyncio.sleep(self._conn_retry_interval)
      except asyncio.CancelledError:
        log.debug("Cancelled")
        return

    self.app = self._app_cls()
    self.app.client = self.client

    try:
      log.debug("App start")
      await self.app.run()
      log.debug("App end")
    except asyncio.CancelledError:
      log.error("Unhandled CancelledError")
    except Exception as ex:
      log.error("Unexpected error")
      log.exception(ex)
    finally:
      self.cancellable = False

      log.debug("Node cleanup")

      try:
        await self.client.close()
      except (asyncio.CancelledError, Exception) as ex:
        log.error("Unexpected error")
        log.exception(ex)

      log.debug("Node end")

      exit_code = self.app.exit_code 
      if exit_code is not None:
        log.info(f"Exit code: {exit_code}")
        sys.exit(exit_code)

  def _on_done(self):
    if self._perform_restart:
      log.info("Restart app")

      self._perform_restart = False
      self.cancellable = True

      self.start()
  
  def on_close(self):
    if self.cancellable:
      log.info("Connection failed")
      log.info("Cancel main task")
      self.main_task.cancel()
      self.cancellable = False
      if self._restart:
        self._perform_restart = True
