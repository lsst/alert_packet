# This file is part of alert_packet.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from contextlib import contextmanager
import io
import unittest
import tempfile
import fastavro
from lsst.alert.packet.io import retrieve_alerts
from lsst.alert.packet.schema import Schema


class RetrieveAlertsTestCase(unittest.TestCase):
    def setUp(self):
        # We could use the real schemas, but that would require building
        # complicated mocks since there are lots of non-null fields in the real
        # schema without defaults, and we'd need to test across versions.
        #
        # Writing our own schema keeps tests simple, and it also checks that
        # retrieve_alerts is independent of any of our schemas' versioning or
        # details.
        self.test_schema_dict = {
            "type": "record",
            "name": "test_record",
            "fields": [
                {"name": "id", "type": "long"},
            ]
        }
        self.test_schema = Schema(self.test_schema_dict)

    def _mock_records(self, n):
        """Return a list of records with mock values, matching self.test_schema.
        """
        return [{"id": i} for i in range(n)]

    @contextmanager
    def _temp_alert_file(self, records):
        with tempfile.TemporaryFile() as alert_file:
            self.test_schema.store_alerts(alert_file, records)
            alert_file.seek(0)
            yield alert_file

    def test_bad_read(self):
        """Check that we throw a useful exception on failure.
        """
        # This should work both if the stream has a name...
        self.assertRaises(RuntimeError, retrieve_alerts,
                          tempfile.NamedTemporaryFile())

        # ...and if it doesn't.
        self.assertRaises(RuntimeError, retrieve_alerts, io.IOBase())

    def test_retrieve_alerts(self):
        """Write some alerts to a file. They should be readable back out.
        """
        records = self._mock_records(5)

        with self._temp_alert_file(records) as alert_file:
            have_schema, have_records_iterable = retrieve_alerts(alert_file, self.test_schema)
            have_records = list(have_records_iterable)

        self.assertEqual(records, have_records)
        self.assertEqual(self.test_schema, have_schema)

    def test_alert_file_with_one_alert(self):
        """Write a single alert to a file. It should be readable back out.
        """
        records = self._mock_records(1)

        with self._temp_alert_file(records) as alert_file:
            have_schema, have_records_iterable = retrieve_alerts(alert_file, self.test_schema)
            have_records = list(have_records_iterable)

        self.assertEqual(records, list(have_records))
        self.assertEqual(self.test_schema, have_schema)

    def test_alert_file_with_no_alerts(self):
        """Write an alert file that contains no alerts at all. It should be readable.
        """
        records = []

        with self._temp_alert_file(records) as alert_file:
            have_schema, have_records_iterable = retrieve_alerts(alert_file, self.test_schema)
            have_records = list(have_records_iterable)

        self.assertEqual(records, list(have_records))
        self.assertEqual(self.test_schema, have_schema)
