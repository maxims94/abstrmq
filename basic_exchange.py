import logging
import asyncio

from .publisher import ExchangePublisher

log = logging.getLogger(__name__)

class BasicExchange:
  """
  BasicQueue represents a AMQP exchange

  This class is NOT used for publishing!
  """

  EXCHANGE_DECLARE_TIMEOUT = 1
  EXCHANGE_BIND_TIMEOUT = 1

  def __init__(self, ch, name='', type='direct'):
    self._ch = ch
    self._name = name
    assert type in ['fanout', 'direct', 'topic', 'headers']
    self._type = type
    self._pub = ExchangePublisher(name, type)
    self.publish = self._pub.publish

  async def declare(self, **kwargs):
    """
    :raises asyncio.TimeoutError:
    """

    log.debug(f"Initialize exchange '{self._name}'")

    if 'timeout' not in kwargs:
      kwargs['timeout'] = self.EXCHANGE_DECLARE_TIMEOUT

    try:
      result = await self._ch.exchange_declare(exchange=self._name, exchange_type=self._type)
    except asyncio.TimeoutError:
      # TODO
      log.error("Exchange declaration timed out")
      raise

  @property
  def name(self):
    return self._name

  async def bind(self, queue: str, **kwargs):
    """
    Creates a binding from this exchange to a queue
    
    :param queue: name of the queue
    :raises asyncio.TimeoutError:
    """

    log.debug(f"Bind '{queue}' to '{self._name}'")

    if 'timeout' not in kwargs:
      kwargs['timeout'] = self.EXCHANGE_BIND_TIMEOUT

    await self._ch.queue_bind(queue, self._name, **kwargs)

class HeadersExchange(BasicExchange):

  def __init__(self, ch, name=''):
    super().__init__(ch, name, 'headers')

  async def bind(self, queue: str, headers: dict = {}, match='all'):
    assert match in ['any', 'all']

    headers['x-match'] = match

    return await super().bind(queue, arguments=headers)
