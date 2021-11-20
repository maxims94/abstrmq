class AbstractService:
  async def run(self):
    """
    Must block for as long as the service is running
    """
    raise NotImplementedError

class AbstractSession:
  async def run(self):
    """
    Must block for as long as the session is running
    """
    raise NotImplementedError
