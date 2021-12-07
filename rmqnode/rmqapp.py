class RMQApp:
  def __init__(self):
    # If this is not None after run(), the node will exit with this exit code
    self.exit_code = None

    # Guaranteed to be set before run() is invoked
    self.client = None

  async def run(self):
    raise NotImplementedError
