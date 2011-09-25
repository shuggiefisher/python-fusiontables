import random
import unittest
from pyft import current_app
from pyft.utils import ft_client_factory

class PYFTLogin(unittest.TestCase):

  def test_connectivity(self):
    print current_app.client.query('show tables')

if __name__ == '__main__':
  unittest.main()
