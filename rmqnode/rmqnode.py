import os
import asyncio
import sys

from .runbase import RunBase
from .rmqapp import RMQApp

from abstrmq import RMQClient

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
    self._restart = restart
    self._max_conn_tries = max_conn_tries
    self._conn_retry_interval = conn_retry_interval

  async def main(self):

    log.debug("Start node")

    self.client = RMQClient()
    num_tries = 0

    while True:
      log.info(f"Try to establish connection (num: {num_tries+1})")

      try:
        await self.client.connect(self._broker_url)
      except asyncio.CancelledError:
        log.debug("Cancelled")
        return
      except Exception as ex:
        log.info(f"Failed to create connection: {ex}")
      else:
        log.info("Successfully created connection")
        break

      num_tries += 1

      if self._max_conn_tries > 0 and num_tries == self._max_conn_tries:
        log.info("Maximal connection attempts reached")
        sys.exit(1)

      log.info(f"Try again in {self._conn_retry_interval}s...")

      try:
        await asyncio.sleep(self._conn_retry_interval)
      except asyncio.CancelledError:
        log.debug("Cancelled")
        return

    try:
      ch = await self.client.channel()
    except:
      pass

    try:
      self.app = self._app_cls()
      self.app.client = self.client
      await self.app.run()

      exit_code = self.app.exit_code 
      if exit_code is not None:
        log.info(f"Exit with code {exit_code}")
        sys.exit(exit_code)

    except asyncio.CancelledError:
      log.debug("App cancelled")
      return
    except Exception as ex:
      log.error("Unexpected error")
      log.exception(ex)

    finally:
      try:
        await self.client.close()
      except Exception as ex:
        log.error("Unexpected error")
        log.exception(ex)

      log.debug("End node")

