import asyncio
from .task_manager import TaskManager
import logging

log = logging.getLogger(__name__)

class SessionManager(TaskManager):
  """
  A session corresponds to a Task

  It is active for as long as the Task is running
  
  Subclass needs to set `SESSION_CLASS` as the class of the session
  It can also use `MAX_SESSIONS` to limit the number of concurrent sessions
  """

  def __init__(self):
    super().__init__()
    self.sessions = []
    self._sema = None
    if hasattr(self, 'MAX_SESSIONS'):
      self._sema = asyncio.Semaphore(self.MAX_SESSIONS)
    self.log = None

  async def new_session(self, *args, **kwargs):
    """
    Creates a new session object, starts it in a new task and returns it
    """
    log.debug("New session")

    assert hasattr(self, 'SESSION_CLASS')
    session = self.SESSION_CLASS(*args, **kwargs)

    if self.log:
      session.log = self.log

    t = self.create_task(session.run())

    if self._sema:
      await self._sema.acquire()

    self.sessions.append(session)
    
    def on_done(fut):
      #log.debug(f"on_done of session {session.corr_id}")
      session.close()

      if self._sema:
        self._sema.release()

      self.sessions.remove(session)

    t.add_done_callback(on_done)

    return session
