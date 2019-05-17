import io
import unittest
import tempfile
from lsst.alert.packet.io import retrieve_alerts

class RetrieveAlertsTestCase(unittest.TestCase):
    def test_bad_read(self):
        """Check that we throw a useful exception on failure.
        """
        # This should work both if the stream has a name...
        self.assertRaises(RuntimeError, retrieve_alerts,
                          tempfile.NamedTemporaryFile())

        # ...and if it doesn't.
        self.assertRaises(RuntimeError, retrieve_alerts, io.IOBase())
