import os

from .runbase import RunBase
from .rmqnodeprocess import RMQNodeProcess

from abstrmq import *

import logging
log = logging.getLogger(__name__)

config = {}
config['BROKER_URL'] = os.environ.get('BROKER_URL', 'amqp://guest:guest@localhost/')

class RMQNodeProcessBase(RunBase):
  """
  `APP` attribute must be an instance of a subclass of RMQNodeProcess
  """

  def __init__(self):
    super().__init__()

    assert hasattr(self, 'APP')
    assert issubclass(self.APP.__class__, RMQNodeProcess)

  async def run(self):
    self.app = None

    try:
      self.client = RMQClient()
      await self.client.connect(config['BROKER_URL'])
    except ConnectionError as ex:
      log.critical("Couldn't create connection")
      return

    ch = await self.client.channel()
    
    self.app = self.APP

    await self.app.init(ch)
    await self.app.run()

  async def cleanup(self):
    # App cleanup happens in `run()` when it is cancelled
    if self.client:
      await self.client.close()
