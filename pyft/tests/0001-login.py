import random
import unittest
from pyft import current_app
from pyft.utils import ft_client_factory

class PYFTLogin(unittest.TestCase):

  def setUp(self):
    self.client = ft_client_factory()

  def test_connectivity(self):
    print self.client.query('show tables')

if __name__ == '__main__':
  unittest.main()
