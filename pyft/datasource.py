from fusiontable.authorization.clientlogin import ClientLogin
from fusiontable.ftclient import ClientLoginFTClient
from fusiontable.sql.sqlbuilder import SQL

from fusiontables.models import FusionTable
from fusiontables.forms import FusionTablesAddForm
from datasource_base import DataSource
from fusiontables import util

import logging

from pyft import fields

logger = logging.getLogger(__name__)

class FusionTable(FusionTableBase):

  _client = None
  _table_id = None
  _db_model = None
  _schema = None
  _django_schema = None
  table_name = None

  def __init__(self, table_name):
    self.table_id = table_id
    self._client = 
    
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
        field = fields.CharField(max_length=128)
      elif col_type == 'location': 
        field = fields.TextField(blank=True, null=True)
      elif col_type == 'string': 
        field = fields.TextField(blank=True, null=True)
      elif col_type == 'number': 
        field = fields.FloatField(blank=True, null=True)
      elif col_type == 'datetime': 
        field = fields.DateTimeField(blank=True, null=True)

      while col_name in schema:
        col_name = col_name + "_"

      schema[util.slugify(unicode(col_name))] = field

    return schema
  
  @classmethod
  def create(cls, schema):
    """
      Create a remote fusion table

      schema: a dictionary representing the table. example:
        {
        "col_name1":"STRING",
        "col_name2":"NUMBER",
        "col_name3":"LOCATION",
        "col_name4":"DATETIME"
        }
    """
    if self._table_id == None:
      if self.table_name == "": raise Exception("Fusion Table name can't be blank")

      resp = self.client.query(
        SQL().createTable(
          { self.table_name: schema }
        ))

      table_id = int(resp.split('\n')[1])

    return cls(table_id=table_id)

  def pull(self):
    "Pull changes from fusion tables"

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

  def push(self, *args, **kwargs):
    "Push data locally back to google-hosted fusion table"

    if('fields' in kwargs ):
      slugged_names = kwargs['fields']
      #look up the fusion table column name for each slugged name
      column_names = [self.columns.get(slugged_name = slugged_name).name for slugged_name in slugged_names]
    else:
      column_names = [column.name for column in self.columns.all()]
      slugged_names = [column.slugged_name for column in self.columns.all()]

    # apply to an optional subset of records.
    if('queryset' in kwargs):
      queryset = kwargs['queryset']
    else:
      queryset = self.db_model.objects.all()

    for row in queryset:
      values = [getattr(row, slugged_name) for slugged_name in slugged_names]
      update_query = SQL().update(self.table_id, column_names, values, int(row.rowid))
      logger.debug('executing fusion table query: %s' % update_query)
      self.client.query(update_query)


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
    
