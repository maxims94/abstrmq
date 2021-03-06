class RMQApp:
  def __init__(self):
    # If this is not None after run(), the node will exit with this exit code
    # This also prevents node restarts
    self.exit_code = None

    # Guaranteed to be set before run() is invoked
    self.client = None

    # The app can define a custom global exception handler
    # Will be used in RunBase
    self.global_exception_handler = None

  async def run(self):
    raise NotImplementedError
