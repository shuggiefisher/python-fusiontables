import re
import uuid

from django.db import models
from django.utils.safestring import mark_safe, SafeData
from django.core.management import color
from django.db import connection

"""
Some tools to dynamically create models. Yoinked from http://code.djangoproject.com/wiki/DynamicModels
"""

def slugify(value):
  """
  Normalizes string, converts to lowercase, removes non-alpha characters,
  and converts spaces to hyphens.
  """
  import unicodedata
  value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
  value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
  return mark_safe(re.sub('[-\s]+', '_', value))


def create_model(name, fields=None, base_model=models.Model, app_label='', module='', options=None, admin_opts=None):
  """
  Create specified model
  """
  class Meta:
    # Using type('Meta', ...) gives a dictproxy error during model creation
    pass

  # explicitly set the django table name, see later note on django weirdness 
  setattr(Meta, 'db_table', name)
  
  if app_label:
    # app_label must be set using the Meta inner class
    setattr(Meta, 'app_label', app_label)

  # Update Meta with any options that were provided
  if options is not None:
    for key, value in options.iteritems():
      setattr(Meta, key, value)

  # Set up a dictionary to simulate declarations within a class
  attrs = {'__module__': module, 'Meta': Meta}

  # Add in any fields that were provided
  if fields:
    attrs.update(fields)

  # Create the class, which automatically triggers ModelBase processing

  # HACK: Django weirdness: django maintains a global cache of model defs
  # in django.db.loading.cache, there is no way (?) to invalidate a model 
  # definition and if the model exists in the cache, the call to __new__ in
  # the model base class short circuits and returns the cached item.
  # To get around this, I create a new cache key by assigning a random 
  # class name to our dynamic model. Downside? a very slow leak in the AppCache.

  class_name = str(uuid.uuid4()).replace('-','')
  model = type(class_name, (base_model,), attrs)

  # Create an Admin class if admin options were provided
  if admin_opts is not None:
    class Admin(admin.ModelAdmin):
      pass
    for key, value in admin_opts:
      setattr(Admin, key, value)
    admin.site.register(model, Admin)

  return model

def install(model):

  # Standard syncdb expects models to be in reliable locations,
  # so dynamic models need to bypass django.core.management.syncdb.
  # On the plus side, this allows individual models to be installed
  # without installing the entire project structure.
  # On the other hand, this means that things like relationships and
  # indexes will have to be handled manually.
  # This installs only the basic table definition.

  # disable terminal colors in the sql statements
  style = color.no_style()

  cursor = connection.cursor()
  statements, pending = connection.creation.sql_create_model(model, style)
  for sql in statements:
    cursor.execute(sql)

def db_table_exists(table, cursor=None):
  try:
    if not cursor:
      cursor = connection.cursor()
    if not cursor:
      raise Exception
    table_names = connection.introspection.get_table_list(cursor)
  except:
    raise Exception("unable to determine if the table '%s' exists" % table)
  else:
    return table in table_names

def remove(model):

  # drop the model's table across all dbs

  # disable terminal colors in the sql statements
  style = color.no_style()

  cursor = connection.cursor()
  statements = connection.creation.sql_destroy_model(model, [], style)
  for sql in statements:
    cursor.execute(sql)

