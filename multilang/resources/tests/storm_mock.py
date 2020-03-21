import sys
from unittest import TestCase
from unittest.mock import MagicMock, patch

class MockBolt:

    def run(self):
        pass

mock = MagicMock()
mock.BasicBolt = MockBolt

sys.modules['storm'] = mock


class Tuple:

    values = []
