import os
import importlib

from pyft.utils import get_cls_by_name
from pyft.utils import ft_client_factory

class App(object):

  settings = None
  loader = None
  client = None

  def __init__(self, loader):
    super(App, self).__init__()
    self.loader = loader
    self.settings = loader.read_configuration()
    u = self.settings['PYFT_GOOGLE_USERNAME']
    p = self.settings['PYFT_GOOGLE_PASSWORD']
    self.client = ft_client_factory(u, p)
    import logging
    logging.basicConfig(filename='/tmp/pyft.log', level=logging.DEBUG)
    logging.debug('Beginning')


def get_app():
  loader_cls = get_cls_by_name(os.environ.get("PYFT_LOADER", "pyft.loader.Loader"))
  return App(loader_cls())

# global app
current_app = get_app()

