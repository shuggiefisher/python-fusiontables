import logging
import datetime
import csv
import time
from urllib2 import HTTPError

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

QUERY_SIZE_LIMIT = 1048576
QUERY_MAX_RATE = datetime.timedelta(milliseconds=200)

class FusionTable(object):

  table_id = None
  client = None
  table_name = None
  _db_model = None
  _schema = None
  _django_schema = None
  _last_query_time = None

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
    "Get a python compatible schema list from fusion table"

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
  def run_query(self, query):

    if self._last_query_time:
      if datetime.datetime.now() - QUERY_MAX_RATE < self._last_query_time:
        td = QUERY_MAX_RATE - (datetime.datetime.now() - self._last_query_time)
        seconds_to_sleep = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6.0
        logger.debug('sleeping : {0}'.format(seconds_to_sleep,td, ) )
        time.sleep(seconds_to_sleep)

    self._last_query_time = datetime.datetime.now()

    logger.debug('execute query: %s' % query)
    if not query:
      return None

    res =  current_app.client.query(query)
    logger.debug('result: %s' % res)
    return res 
  
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
    resp = self.run_query(drop_sql)
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

  def get_unique_keys(self, rows):
    # assume all the rows are the same
    return rows[0].unique_keys

  def determine_which_rows_are_not_inserted(self, rows):
    """
    Return the list of rows not present in the fusion table
    and the rowid's of those rows present
    """

    unique_keys = self.get_unique_keys(rows)
    # Use the first unique key
    key_column_name = unique_keys[0].column_name 
    in_clause = {key_column_name:[row.field_lookup[key_column_name].prepare_value() for row in rows]}
    logger.debug('determine_rows_inserted: using column_key {0}'.format(unique_keys[0]))

    headers, results = self.select([x.column_name for x in unique_keys] + ['rowid'], in_clause)

    logger.debug('seeing these columns {0} and results {1}'.format(headers, results))

    result_index = headers.index(key_column_name)
    key_results = [result[result_index] for result in results]

    rows_not_present = []
    for row in rows:
      if row.field_lookup[key_column_name].prepare_value() not in key_results:
        rows_not_present.append(row)
    return rows_not_present


  def insert(self, rows=[]):
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

    def execute_query_list(ql, handle_500_exception=True):
      new_row_ids = []
      logger.debug('running query list insert of {0} queries'.format(len(ql)))
      query_list, row_list = zip(*ql)
      try:
        result = self.run_query(";".join(query_list))
      except HTTPError, e:
        logger.debug('Insert Error State!', e)
        if e.code == 500 and handle_500_exception:
          # XXX Fusion Table throws 500 for successful inserts
          logger.debug('500 Insert Error')
          # did google fusion tables barf?
          # check if our rows are present
          reinsert_these_rows = self.determine_which_rows_are_not_inserted()
          insert_queries = [SQL().insert(self.table_id, row.field_lookup) for row in reinsert_these_rows]
          execute_query_list(insert_queries, handle_500_exception=False)
          headers, result_rows = self.client.query("select ")


      rows = result.strip().split('\n')
      # skip the header
      for row in csv.reader(rows[1:]):
        new_row_ids += row
      return new_row_ids

    query_list = []
    # assume all the rows are the same
    unique_keys = self.get_unique_keys(rows)
    row_ids = []
    logger.debug('starting bulk insert of {0} queries'.format(len(query_list)))

    for row in rows:
      query = SQL().insert(self.table_id, row.field_lookup)

      if len(";".join([t[0] for t in query_list] + [query])) > QUERY_SIZE_LIMIT \
          or len(query_list) >= 500:
        # execute the queued up queries
        row_ids += execute_query_list(query_list)
        logger.debug('Hit critical limit, size:{0},len:{1}'.format(len(";".join([t[0] for t in query_list] + [query])),
          len(query_list)))
        query_list = [(query,row)]
      else:
        query_list.append((query,row))

    # clean up any remaining queries
    if query_list:
      row_ids += execute_query_list(query_list)

    return row_ids

  def select(self, select_columns=None, in_clause={}):
    """
    Based on a set of rows, get the remote fusion table row_ids
    """

    if select_columns is None:
      select_columns = self.base_schema.keys()

    membership_clauses = []
    for key in in_clause.keys():
      membership_clauses.append("'{0}' IN ({1})".format(key, ",".join(in_clause[key])))

    select_query = SQL().select(self.table_id, cols=select_columns, condition="AND".join(membership_clauses))
    return self.parse_row_results(self.run_query(select_query))

  def parse_row_results(self, results):
    rows = results.strip().split('\n')
    parsed_rows = []
    # skip the header
    for row in csv.reader(rows):
      parsed_rows.append(row)
    # return the header, and the rows
    return parsed_rows[0], parsed_rows[1:]

  def update(self, rows=[]):
    """
    Push data locally back to google-hosted fusion table
    `rows` is a list of Row objects
    """

    query_list = []
    for row in rows:
      if row.row_id is None:
        raise AttributeError('Rows must have a `row_id` set on UPDATE')

      column_name_field_map = {}
      unique_columns = []
      values = []

      for field in row:
        if field.unique_key:
          unique_columns.append(field.column_name)

      update_query = SQL().update(self.table_id, 
                                  row.column_names(), 
                                  row.values(), 
                                  int(row.row_id))

      query_list.append(update_query)

    result_batch = []
    for query in query_list:
      result_batch.append(self.run_query(query))
    logger.debug('BULk UPDATE result batch {0}'.format(result_batch) )
    return result_batch


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
    
