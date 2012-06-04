import logging
logger = logging.getLogger(__name__)

#class LocalStore(object):
#  def schema(self):
#    raise NotImplementedError()
#  def rows(self):
#    raise NotImplementedError()
#class CSVStore(LocalStore):
#  def __init__(self, csv_file):
#    self.csv_file = csv_file
#  def schema(self):
#    pass


class Row(list):

  def __init__(self, row_id=None, fields=[]):
    res = super(Row, self).__init__()
    self.row_id = row_id
    self.unique_keys = []
    self.field_lookup = {}
    for field in fields:
      self.append(field)
      self.field_lookup[field.column_name] = field.prepare_value()
      if field.unique_key:
        self.unique_keys.append(field)
    return res

  def column_names(self):
    return [field.column_name for field in self]

  def values(self):
    return [field.value for field in self]
  
  def field_lookup(self, column_name):
    pass

class BaseField(object):

  def __init__(self, value=None, ft_column_id=None, column_name=None, column_type=None, unique_key=False):

    self.value = value

    self.ft_column_id = ft_column_id
    self.column_name = column_name
    self.column_type = column_type
    self.unique_key = unique_key

  def prepare_value(self):
    return "{0}".format(self.value)

class RowID(BaseField):
  column_type = "RowID"

class StringField(BaseField):
  column_type = "STRING"

  def prepare_value(self):
    return "'{0}'".format(self.value)

class NumberField(BaseField):
  column_type = "NUMBER"

  def prepare_value(self):
    if type(self.value).__name__ == 'int':
      return "{0:d}".format(self.value)
    #XXX round off:
    elif type(self.value).__name__ == 'float':
      return "{0:f}".format(self.value)
    elif type(self.value).__name__ == 'Decimal':
      return "%s" % self.value

class LocationField(StringField):
  column_type = "LOCATION"

class DatetimeField(BaseField):
  column_type = "DATETIME"

  def prepare_value(self):
    return "%s" % self.value