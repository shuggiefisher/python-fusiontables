import os
from pyft.utils import import_from_cwd
from pyft.utils import find_module

DEFAULT_CONFIG_MODULE = "pyftconfig"

class Loader(object):
  "from celery loader"

  def setup_settings(self, settingsdict):
    return settingsdict

  def find_module(self, module):
    return find_module(module)

  def read_configuration(self):
    """Read configuration from :file:`pyftconfig.py` and configure
    pyft and Django so it can be used by regular Python."""
    configname = os.environ.get("PYFT_CONFIG",
                   DEFAULT_CONFIG_MODULE)
    try:
      self.find_module(configname)
    except ImportError:
      warnings.warn(NotConfigured(
        "No %r module found! Please make sure it exists and "
        "is available to Python." % (configname, )))
      return self.setup_settings({})
    else:
      pyftconfig = import_from_cwd(configname)
      usercfg = dict((key, getattr(pyftconfig, key))
              for key in dir(pyftconfig)
                if self.wanted_module_item(key))
      self.configured = True
      return self.setup_settings(usercfg)

  def wanted_module_item(self, item):
    return item[0].isupper() and not item.startswith("_")


