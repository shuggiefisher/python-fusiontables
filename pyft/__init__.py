import os
import importlib

from pyft.utils import get_cls_by_name

class App(object):

  settings = None
  loader = None

  def __init__(self, loader):
    super(App, self).__init__()
    self.loader = loader
    self.settings = loader.read_configuration()


def get_app():
  loader_cls = get_cls_by_name(os.environ.get("PYFT_LOADER", "pyft.loader.Loader"))
  return App(loader_cls())

# global app
current_app = get_app()
