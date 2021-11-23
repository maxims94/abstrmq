import asyncio
import aiormq
import aiormq.abc
import logging
import json

log = logging.getLogger(__name__)

from .helper import _dict_subset
from .exceptions import *

from .basic_exchange import BasicExchange

class QueueMessage:
  def __init__(self, message:aiormq.abc.DeliveredMessage):
    self.body = message.body.decode()
    properties = message.header.properties
    self.corr_id = properties.correlation_id
    self.reply_to = properties.reply_to
    self.content = json.loads(self.body)
    assert isinstance(self.content, dict)
    self.delivery_tag = message.delivery.delivery_tag
    self.ch = message.channel
    self._message = message

  async def ack(self):
    """
    TODO
    :raises: ?
    """
    log.debug("Ack message: %s", self.delivery_tag)
    await self.ch.basic_ack(self.delivery_tag)

  def __str__(self):
    return str(self.content)
  
  def __repr__(self):
    return f"<QueueMessage: {self.delivery_tag}>"

  def match_dict(self, sub):
    return _dict_subset(sub, self.content)

  def assert_message(self, callable, msg='Invalid message'):
    if not callable(self):
      raise InvalidMessageError(msg)
    return True

  def assert_content(self, callable, msg='Invalid message content'):
    if not callable(self.content):
      raise InvalidMessageError(msg)
    return True

  def assert_reply_to(self):
    if self.reply_to is None:
      raise InvalidMessageError("Message requires reply_to")
    return True

  def assert_corr_id(self):
    if self.corr_id is None:
      raise InvalidMessageError("Message requires corr_id")
    return True

  def assert_equals(self, expected, msg='Invalid message content'):
    if self.content != expected:
      raise InvalidMessageError(msg)
    return True
  
  def get(self, key):
    if not key in self.content:
      raise InvalidMessageError(f"Expected key '{key}' not found")
    else:
      return self.content[key]

class BasicQueue:
  """
  BasicQueue represents a AMQP queue with a single consumer
  """

  QUEUE_DECLARE_TIMEOUT = 1
  QUEUE_DELETE_TIMEOUT = 1
  QUEUE_PURGE_TIMEOUT = 1

  def __init__(self, ch, queue=''):
    """
    :param queue: the name of the queue, empty string will create an anonymous queue
    """
    self._ch = ch
    self._queue = queue
    # Name of the confirmed / generated queue
    self._name = None
  
  # TODO: change interface to declare, delete, purge?
  async def init_queue(self, **kwargs):
    """
    :raises asyncio.TimeoutError:
    """
    log.debug("Initialize queue")

    if 'timeout' not in kwargs:
      kwargs['timeout'] = self.QUEUE_DECLARE_TIMEOUT
    result = await self._ch.queue_declare(queue=self._queue, **kwargs)
    self._name = result.queue

  async def delete_queue(self):
    assert self._name
    log.debug("Delete queue")
    await self._ch.queue_delete(queue=self._name, timeout=self.QUEUE_DELETE_TIMEOUT)
    # TODO: unsubscribe consumer, i.e. run cancel_consume? Or is this done automatically?
    # TODO: the default is if_unused=True; if there is a subscriber, then that means it won't delete it?
    # TODO: does this delete all messages or only the queue itself?

  async def purge_queue(self):
    assert self._name
    #log.debug("Purge queue")
    await self._ch.queue_purge(queue=self._name, timeout=self.QUEUE_PURGE_TIMEOUT)

  async def start_consume(self, on_message, **kwargs):
    assert self._name
    log.debug("Consume queue")
    self.on_message = on_message
    # TODO: set as exclusive consumer
    result = await self._ch.basic_consume(self._name, self._on_message, **kwargs)
    self._consumer_tag = result.consumer_tag

  @property
  def name(self):
    return self._name

  async def cancel_consume(self):
    assert self._consumer_tag
    await self._ch.basic_cancel(self._consumer_tag)

  async def _on_message(self, message):
    # In case parsing the message fails, we still want to know that we received a message
    log.debug("Received message (delivery_tag: %s)", message.delivery.delivery_tag)
    qm = QueueMessage(message)
    log.debug("Content: %s", qm.content)
    log.debug(f"Properties: corr_id={qm.corr_id}, reply_to={qm.reply_to}, delivery_tag={qm.delivery_tag}")
    await self.on_message(qm)
