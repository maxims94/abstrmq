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
  EXCHANGE_UNBIND_TIMEOUT = 1

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
    Create a binding from this exchange to a queue

    NOTE: We define `bind` as a method of an exchange since e.g. a HeadersExchange has its own bind logic (while it is the same for any queue)

    NOTE: Use the queue's name as argument instead of an object to keep it flat and decoupled
    
    :param queue: name of queue
    :raises: asyncio.TimeoutError
    """

    if 'arguments' in kwargs:
      header_str = ", ".join(f"{k}={v}" for k,v in kwargs['arguments'].items())
      header_str = " with headers {" + header_str + "}"
      log.debug(f"Bind '{queue}' to '{self._name}'" + header_str)
    else:
      log.debug(f"Bind '{queue}' to '{self._name}'")

    # TODO: better log; "with binding key" (e.g. for direct exchange)

    if 'timeout' not in kwargs:
      kwargs['timeout'] = self.EXCHANGE_BIND_TIMEOUT

    await self._ch.queue_bind(queue, self._name, **kwargs)

  async def unbind(self, queue: str, **kwargs):
    """
    Undoes a binding

    :param queue: name of the queue
    :raises asyncio.TimeoutError:
    """

    log.debug(f"Unbind '{queue}' from '{self._name}'")

    if 'timeout' not in kwargs:
      kwargs['timeout'] = self.EXCHANGE_UNBIND_TIMEOUT

    await self._ch.queue_unbind(queue, self._name, **kwargs)

class HeadersExchange(BasicExchange):

  def __init__(self, ch, name=''):
    super().__init__(ch, name, 'headers')

  async def bind(self, queue: str, headers: dict = {}, match='all'):
    assert match in ['any', 'all']

    headers = headers.copy()
    headers['x-match'] = match

    return await super().bind(queue, arguments=headers)

  async def unbind(self, queue: str, headers: dict = {}, match='all'):
    assert match in ['any', 'all']

    headers = headers.copy()
    headers['x-match'] = match

    return await super().unbind(queue, arguments=headers)
