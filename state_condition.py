from asyncio import Condition
from enum import Enum

class StateCondition:
  """
  A synchronization primitive that represents a finite state machine
  """

  def __init__(self, cls, init, init_data = None):
    """
    :param cls: A subclass of Enum that represents all possible states
    :param init: Initial state 
    :param init_data: Optionally, some state data
    """
    assert issubclass(cls, Enum)
    assert isinstance(init, cls)
    self._cls = cls
    self._state = init
    self._data = init_data
    self._cond = Condition()

  def is_in(self, *states):
    return any(self._state is state for state in states)
  
  async def wait_for(self, *states):
    """
    :param states: Wait for a state transition to subset of states. If empty, wait for ANY state transition.
    """
    async with self._cond:
      if states:
        return await self._cond.wait_for(lambda: self._state in states)
      else:
        return await self._cond.wait()
  
  async def set(self, state, data = None):
    """
    Set to a new state. May be the same as the current state.
    """
    assert isinstance(state, self._cls)

    self._state = state
    self._data = data

    async with self._cond:
      self._cond.notify_all()

  @property
  def data(self):
    return self._data

  @property
  def current(self):
    return self._state

  def __repr__(self):
    return f'<{self.__class__.__name__}, {self._state.value}, {self._data}>'
