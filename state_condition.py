from asyncio import Condition
from enum import Enum

class StateCondition:
  """
  A synchronization primitive that represents a finite state machine
  """

  def __init__(self, cls, init):
    """
    :param cls: A subclass of Enum that represents all possible states
    :param init: Initial state 
    """
    assert issubclass(cls, Enum)
    assert isinstance(init, cls)
    self._cls = cls
    self._state = init
    self._cond = Condition()

  def is_in(self, *states):
    return any(self._state is state for state in states)
  
  async def wait_for(self, *states):
    async with self._cond:
      return await self._cond.wait_for(lambda: self._state in states)
  
  async def set(self, state):
    assert isinstance(state, self._cls)
    assert self._state != state

    self._state = state
    async with self._cond:
      self._cond.notify_all()

  @property
  def current(self):
    return self._state
