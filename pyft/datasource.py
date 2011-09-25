import logging

from pyft import current_app
from pyft.client.sql.sqlbuilder import SQL

from pyft.fields import RowID
from pyft.fields import StringField
from pyft.fields import NumberField
from pyft.fields import LocationField
from pyft.fields import DatetimeField

logger = logging.getLogger(__name__)

DEFAULT_TYPE_HANDLER = {
    'rowid': RowID,
    'string':StringField,
    'number':NumberField,
    'location':LocationField,
    'datetime':DatetimeField
    }

class FusionTable(object):

  table_id = None
  client = None
  table_name = None
  _db_model = None
  _schema = None
  _django_schema = None

  def __init__(self, table_id, type_handler=DEFAULT_TYPE_HANDLER, name_handler={}):
    self.table_id = table_id
    self.client = current_app.client
    self.column_handler_by_type = type_handler
    self.column_handler_by_name = name_handler
    
  @property
  def table_name(self):

    # parse resp into rows(\n) and columns(,),
    # discarding the first row which is header info
    tbl_attrs = []
    query_res = self.client.query("show tables")
    for table_line in query_res.strip().split("\n")[1:]:
      tbl_attrs.append(table_line.split(",")) 

    # cast table ids as int
    tbl_ids = [int(tbl_attr[0]) for tbl_attr in tbl_attrs]
    
    this_tbl_idx = tbl_ids.index(self.table_id)
    self.table_name = tbl_attrs[this_tbl_idx][1]

    return self.table_name

  def set_table_name(self):

    # parse resp into rows(\n) and columns(,),
    # discarding the first row which is header info
    tbl_attrs = []
    query_res = self.client.query("show tables")
    for table_line in query_res.strip().split("\n")[1:]:
      tbl_attrs.append(table_line.split(",")) 

    # cast table ids as int
    tbl_ids = [int(tbl_attr[0]) for tbl_attr in tbl_attrs]
    
    this_tbl_idx = tbl_ids.index(self.table_id)
    self.table_name = tbl_attrs[this_tbl_idx][1]

    return self.table_name

  def fetch_schema(self):
    "returns a list that represents the fusion table schema"

    description = self.client.query(SQL().describeTable(self.table_id))
    self._schema = [('rowid','rowid','rowid')] 
    self._schema += [tuple(row.split(",")) for row in description.split("\n")][1:-1] #skips header row

    return self._schema

  @property
  def base_schema(self):
    """
      Get a python compatible schema list from fusion table
    """

    description = self.client.query(SQL().describeTable(self.table_id))
    schema = {}
    for col_id, col_name, col_type in self.fetch_schema():

      if col_type == 'rowid': 
        field = RowID
      elif col_type == 'location': 
        field = LocationField
      elif col_type == 'string': 
        field = StringField
      elif col_type == 'number': 
        field = NumberField
      elif col_type == 'datetime': 
        field = DatetimeField

      while col_name in schema:
        col_name = col_name + "_"

      schema[unicode(col_name)] = field

    return schema
  
  @classmethod
  def create(cls, schema, table_name, type_handler=DEFAULT_TYPE_HANDLER, name_handler={}):
    """
      Create a remote fusion table

      schema: a dictionary representing the remote table. example:
        {
        "col_name1":"STRING",
        "col_name2":"NUMBER",
        "col_name3":"LOCATION",
        "col_name4":"DATETIME"
        }
    """
    resp = current_app.client.query(
      SQL().createTable(
        { table_name: schema }
      ))
    table_id = int(resp.split('\n')[1])

    return cls(table_id)

  def delete(self):
    " Delete this remote fusion table "
    drop_sql = SQL().dropTable(self.table_id)
    resp = current_app.client.query(drop_sql)
    return resp

  def pull(self):
    " Pull changes from fusion tables "

    import time
    start_time = time.clock()
    ft = self.data_input.data_ref

    ft.update_table_name()

    if util.db_table_exists(ft.db_model._meta.db_table):
      ft.remove_local_store()

    ft.install_local_store()
    ft.pull()
    end_time = time.clock()
    logger.info("synchronized {0} in {1} seconds (CPU time)".format(ft.table_name, end_time - start_time));

  def update(self, rows=[], callback=None):
    """Push data locally back to google-hosted fusion table
       `rows` is a list of Row objects
    """

    # apply to an optional subset of records.

    for row in rows:
      column_names = []
      column_name_field_map = {}
      unique_columns = []
      values = []

      for field in row:
        column_names.append(field.column_name)
        column_name_field_map[field.column_name] = field
        if field.unique_key:
          unique_columns.append(field.column_name)
        values.append(row.field_value)

      update_query = SQL().update(self.table_id, column_names, values, int(row.rowid))
      logger.debug('executing fusion table query: %s' % update_query)
      self.client.query(update_query)

  def insert(self, rows=[], callback=None):
    """
      Push data locally back to google-hosted fusion table
       `rows` is a list of Row objects that have the same column structure

      schema: a dictionary representing the rows to be inserted. example:
        {
        "col_name1":"STRING",
        "col_name2":"NUMBER",
        "col_name3":"LOCATION",
        "col_name4":"DATETIME"
        }

    """

    # apply to an optional subset of records.

    for row in rows:
      insert_values = {}
      for col_name in row.column_names():
        # don't call prepare_value() because `SQL` does it for us
        insert_values[col_name] = row.field_lookup[col_name].value

      insert_query = SQL().insert(self.table_id, insert_values)
      logger.debug('executing fusion table query: %s' % insert_query)
      self.client.query(insert_query)

    # now fetch the fusion table rowid for each row
    if callback:
      ids = self.get_row_ids(rows)

  def get_row_ids(self, unique_key, rows=[]):
    """
    Based on a set of rows, get the remote fusion table rowids
    `rows` are a set of row objects
    `unique_key` is the name of a column that acts as a primary key in our rows
    """

    # build a lookup mapping for key-column-name -> key-value -> row
    key_field_map = {}
    key_values = []
    for row in rows:
      for field in row:
        if field.column_name == unique_key:
          key_values.append(field.value)
          key_field_map[field.value] = row 
   
    logger.debug("Produced key-field map:{0}".format(key_field_map))
    select_columns = rows[0].column_names() + ['ROWID']
    logger.debug("selecting these columns:{0}".format(select_columns))
    key_column_values = ",".join([r.field_lookup[unique_key].prepare_value() for r in rows])
    membership_clause = "'{0}' IN ({1})".format(unique_key,
                                                key_column_values)

    select_query = SQL().select(self.table_id, select_columns, membership_clause)
    logger.debug('executing fusion table query: %s' % select_query)
    return self.client.query(select_query)


  def pull(self):
    """
      This will pull down all the data from the remote fusion table and put it in our db
    """
    col_qs = self.columns.all()
    col_ids = [x['col_id'] for x in col_qs.values('col_id')]

    select_fields = ','.join(col_ids)

    query_string = "select %s from %s"%(select_fields, self.table_id)
    rows = self.client.query(query_string).strip().split('\n')

    model_class = self.db_model
    error_messages = []
    for row in csv.DictReader(rows):
      slugged_row = {}
      for column in col_qs:
        if column.column_type == 'datetime':
          row[column.name] = parser.parse(row[column.name])
        if column.column_type == 'number': #FIXME we shouldn't bury this error here. We're only ignoring it for expediency of managing our shite data.
          try:
            row[column.name] = float(row[column.name])
          except:
            row[column.name] = None
        slugged_row[column.slugged_name] = row[column.name]
       
      # instantiate a model for the corresponding django class

      model = model_class()
      # update all attributes from the fusion table row
      try:
        model.__dict__.update(slugged_row)
        model.save()
      except ValidationError as e:
        message = 'Cannot insert FusionTable row into our DB: {1}'.format(e)
        error_messages.append(message)
        logger.error(message)
        logger.debug(row)

    return error_messages        
    
