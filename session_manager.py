import asyncio
from .task_manager import TaskManager
import logging
from .abstract_service import AbstractSession

log = logging.getLogger(__name__)

class SessionManager(TaskManager):
  """
  A session corresponds to a Task running its 'run' coro

  It is active for as long as that task is running

  To close a SessionManager, use TaskManager.close()!

  Two ways to use session objects
  * MySession(AbstractSession)
      * A session is a complex set of tasks with a main `run` task
      * Within these tasks, real sessions can be created
      * When receiving or publishing, you use other session objects (usually, FutureQueueSession)
  * MySession(FutureQueueSession, AbstractSession)
      * It still has a main `run` task
      * But it is also a real session in the sense that it has a corr_id
      * You use `self.receive` and `self.publish` for publishing
  """

  def __init__(self, max_sessions=None):
    super().__init__()

    self.sessions = []
    self._sema = None
    if max_sessions is not None:
      self._sema = asyncio.Semaphore(self.MAX_SESSIONS)
    self.log = None

  async def new_session(self, session):
    """
    :param session: AbstractSession instance

    TODO: raises?
    """
    log.debug("New session")

    assert hasattr(session, 'run')

    if self.log:
      session.log = self.log

    t = self.create_task(session.run())

    if self._sema:
      await self._sema.acquire()

    self.sessions.append(session)
    
    def on_done(fut):
      log.debug(f"on_done of session '{str(session)}'")

      session.close()

      if self._sema:
        self._sema.release()

      self.sessions.remove(session)

    t.add_done_callback(on_done)

    return session
  
  def is_full(self):
    assert self._sema
    return self._sema.locked()
