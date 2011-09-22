import os
import sys

import imp as _imp
import importlib
from contextlib import contextmanager

from pyft.client.authorization.clientlogin import ClientLogin
from pyft.client.ftclient import ClientLoginFTClient
from pyft.client.sql.sqlbuilder import SQL


@contextmanager
def cwd_in_path():
  cwd = os.getcwd()
  if cwd in sys.path:
    yield
  else:
    sys.path.insert(0, cwd)
    try:
      yield cwd
    finally:
      try:
        sys.path.remove(cwd)
      except ValueError:
        pass

def find_module(module, path=None, imp=None):
  """Version of :func:`imp.find_module` supporting dots."""
  if imp is None:
    imp = importlib.import_module
  with cwd_in_path():
    if "." in module:
      last = None
      parts = module.split(".")
      for i, part in enumerate(parts[:-1]):
        path = imp(".".join(parts[:i + 1])).__path__
        last = _imp.find_module(parts[i + 1], path)
      return last
    return _imp.find_module(module)

def get_cls_by_name(name, aliases={}, imp=None):
  """Get class by name. Taken from celery project

  The name should be the full dot-separated path to the class::

    modulename.ClassName

  If `aliases` is provided, a dict containing short name/long name
  mappings, the name is looked up in the aliases first.

  """
  if imp is None:
    imp = importlib.import_module

  if not isinstance(name, basestring):
    return name                 # already a class

  name = aliases.get(name) or name
  module_name, _, cls_name = name.rpartition(".")
  try:
    module = imp(module_name)
  except ValueError, exc:
    raise ValueError("Couldn't import %r: %s" % (name, exc))
  return getattr(module, cls_name)

get_symbol_by_name = get_cls_by_name

def import_from_cwd(module, imp=None):
    """Import module, but make sure it finds modules
    located in the current directory.

    Modules located in the current directory has
    precedence over modules located in `sys.path`.
    """
    if imp is None:
        imp = importlib.import_module
    with cwd_in_path():
        return imp(module)

def ft_client_factory():

  from pyft import current_app
  username = current_app.settings['PYFT_GOOGLE_USERNAME']
  password = current_app.settings['PYFT_GOOGLE_PASSWORD']

  token = ClientLogin().authorize(username, password)
  client = ClientLoginFTClient(token)
  return client


# VALIDATION

import re
from urllib2 import HTTPError

def validate_ft_input_string(input_str):
  """
  takes in a comma seperated list of 0 or more numeric ids:"123,321,44"
  and urls: "http://www.google.com/fusiontables/DataSource?dsrcid=233"
  you can also mix them since we know that google has no commas in their
  fusiontable query params ;)
  "http://www.google.com/fusiontables/DataSource?dsrcid=233,123,321"
  """

  id_list = [s.strip() for s in input_str.split(',')]
  accum = []
  for potential_id in id_list:
      if potential_id.find('google.com') > 1:
          accum.append((potential_id,re.search('dsrcid=([^(?|&)]*)', potential_id).group(1)))
      else:
          accum.append((potential_id,potential_id))
  id_list = []
  for source_id_frag, id in accum:
    try:
      id_list.append(int(id))
    except ValueError:
      msg = "Error processing '%s' output fragment is not a numeric id: '%s'\n"%(source_id_frag, id)
      msg += "Type in fusion table urls or ids separated by commas"
      raise forms.ValidationError(msg)
  return validate_ft_id_list(id_list)


def validate_ft_id_list(input_ft_id_list):
  ft = ft_client_factory()
  unauthorized_ids = []
  error_ids = []
  for id in input_ft_id_list:
    try:
      ft.query("describe %s"%id)
    except HTTPError, e:
      if e.code == 401:
        unauthorized_ids.append(id)
      else:
        error_ids.append(id)

  if len(unauthorized_ids) > 0:
    errors = ", ".join(["%s"%x for x in unauthorized_ids])
    message = "I do not have permission to access these tables: \"%s\"\n"%errors
    message += "Please invite %s@gmail.com to share your tables"%PYFT_GOOGLE_USERNAME
    raise forms.ValidationError(message)
  if len(error_ids) > 0:
    errors = ", ".join(["%s"%x for x in error_ids])
    message = "An error occured accessing access these tables: \"%s\"\n"%errors
    raise forms.ValidationError(message)

  return input_ft_id_list

