import asyncio
import aiormq
import logging
import json
import inspect

log = logging.getLogger(__name__)

class BasicPublisher:
  """
  Encapsulates the AMQP publishing mechanism

  Use this class directly if you need a generic publisher or use any of the subclasses for more specific publishers
  """

  PUBLISH_TIMEOUT = 1
  MAX_LOG_LEN = 500

  def __init__(self, ch):
    self._ch = ch
    # Default reply_to
    self.reply_to = None

  async def publish(self, message, exchange='', routing_key='', reply_to=None, persistent=False, corr_id=None, timeout=None, headers={}):
    """
    Publishes a message. Only only publishes with a timeout

    Raises TimeoutError

    :param message: a dict that will be encoded as JSON
    :param timeout:
    """

    msg_str = str(message)
    if len(msg_str) > self.MAX_LOG_LEN:
      msg_str = msg_str[:self.MAX_LOG_LEN-3]+"..."
    log.info('Publish message: %s', msg_str)

    body = json.dumps(message).encode()

    reply_to = reply_to or self.reply_to

    delivery_mode = 1
    if persistent:
      delivery_mode = 2

    timeout = timeout or self.PUBLISH_TIMEOUT

    frame = inspect.currentframe()
    args, _, _, locals = inspect.getargvalues(frame)
    props = {arg : locals[arg] for arg in args[2:]}
    log.debug('Message properties: %s', str(props))

    try:
      await self._ch.basic_publish(
        body,
        exchange = exchange,
        routing_key = routing_key,
        properties = aiormq.spec.Basic.Properties(
          correlation_id = corr_id,
          delivery_mode = delivery_mode,
          reply_to = reply_to,
          headers = headers
          ),
        timeout = timeout
      )
    except asyncio.TimeoutError:
      log.warning("Timeout while publishing")
      raise

class DirectPublisher(BasicPublisher):
  """
  Publish directly to a queue using the default exchange
  """
  
  QUEUE_DECLARE_TIMEOUT = 1

  def __init__(self, ch, dest, reply_to=None):
    """
    :param dest: name of the destination queue (str)
    """
    self._ch = ch
    self._dest = dest
    self.reply_to = reply_to

  # TODO: Do you use this? or instead use the resource class BasicQueue directly to avoid redundancies?
  async def init_dest_queue(self, **kwargs):
    """
    Initialize the destination queue (make sure that it exists)

    :raises: TimeoutError
    """
    # TODO: passive=true should only ensure that it exists (throw an error if not)

    kwargs['queue'] = self._dest
    if 'timeout' not in kwargs:
      kwargs['timeout'] = self.QUEUE_DECLARE_TIMEOUT
    result = await self._ch.queue_declare(**kwargs)

  async def publish(self, message, **kwargs):
    await super().publish(message, exchange='', routing_key=self._dest, **kwargs)

class ExchangePublisher(BasicPublisher):

  # TODO: check whether exchange exists? Or assume that?

  def __init__(self, ch, exchange: str, reply_to=None):
    self._ch = ch

    assert isinstance(exchange, str)
    self._exchange = exchange

    self.reply_to = reply_to

  async def publish(self, message, **kwargs):
    await super().publish(message, exchange=self._exchange, **kwargs)

class HeadersExchangePublisher(ExchangePublisher):

  async def publish(self, message, headers={}, **kwargs):
    await super().publish(message, headers=headers, **kwargs)
