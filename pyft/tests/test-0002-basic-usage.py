import random
import unittest
import string

from pyft import current_app
from pyft.fusiontables import FusionTable
from pyft.fields import NumberField
from pyft.fields import StringField
from pyft.fields import RowID
from pyft.fields import Row
import logging
logger = logging.getLogger(__name__)

class PYFTUsage(unittest.TestCase):

  def setUp(self):

    self.schema = {'letters': StringField.column_type,
                   'numbers':NumberField.column_type}
    self.table_name = current_app.settings['TEST_TABLE_PREFIX'] + "PYFTUsage"
    self.ft = FusionTable.create(self.schema, self.table_name)

    self.simple_data = [(-1, 'minus1'),(-2,'minus2'),(-3,'minus3')]
    self.dummy_data = [(n,l) for n,l in  zip(*self.build_ran_data())]

  def build_ran_data(self):

    rand_numbers = []
    rand_letters = []

    for i in xrange(2000):
      ran_str = ""

      for k in xrange(100):
        ran_str += (string.letters + string.digits)[random.randrange(0, len(string.letters + string.digits))]

      rand_numbers.append(random.randint(0,1000))
      rand_letters.append(ran_str)

    return rand_numbers, rand_letters

  def test_simple_insert_and_update(self):

    rows = []
    for num,letter in self.simple_data:
      rows.append(Row(row_id=None, fields=[NumberField(num, column_name="numbers"),
                                           StringField(letter,column_name="letters")]))
    row_ids = self.ft.insert(rows)
    rows = []
    for (num,letter),row_id in zip(self.simple_data, row_ids):
      rows.append(Row(row_id=row_id, fields=[NumberField(num, column_name="numbers"),
                                           StringField(letter,column_name="letters")]))

    row_ids = self.ft.update(rows)

    # from IPython import embed
    # embed()

  def test_large_insert_and_update(self):

    rows = []
    for num,letter in self.dummy_data:
      rows.append(Row(row_id=None, fields=[NumberField(num, column_name="numbers"),
                                           StringField(letter,column_name="letters")]))
    row_ids = self.ft.insert(rows)
    assert(len(row_ids) == len(rows))

    rows = []
    update_these =  zip(self.dummy_data[:10], row_ids[:10])
    assert(len(update_these) == 10)
    logger.debug('updating these {0}'.format(update_these))
    for (num,letter),row_id in update_these:
      rows.append(Row(row_id=row_id, fields=[NumberField(num, column_name="numbers"),
                                           StringField("updated-"+letter,column_name="letters")]))

    row_ids = self.ft.update(rows)

    # from IPython import embed
    # embed()
    
  def test_schema_fetch(self):

    res = self.ft.base_schema

  def tearDown(self):

    self.ft.delete()

if __name__ == '__main__':
  unittest.main()
