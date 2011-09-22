import random
import unittest
from pyft import current_app
from pyft.utils import ft_client_factory

class PYFTUsage(unittest.TestCase):

  def setUp(self):
    self.client = ft_client_factory()

  def test_connectivity(self):
    pass

if __name__ == '__main__':
  unittest.main()
