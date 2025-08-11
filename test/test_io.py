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
import posixpath
import fastavro  # noqa: F401
import json
from lsst.alert.packet.io import retrieve_alerts
from lsst.alert.packet.schema import Schema
from lsst.alert.packet import get_latest_schema_version, get_schema_path


class RetrieveAlertsTestCase(unittest.TestCase):
    def setUp(self):
        """Prepare the test with sample alerts.

        It's important to use the actual alert schema here because the
        retrieve_alerts function calls Schema.__init__, which resets fastavro's
        SCHEMA_DEFS cache. That cache only gets used for complex schemas which
        use named references to types, so a simple mock record type is not
        sufficient.
        """
        self.test_schema_version = get_latest_schema_version()
        self.test_schema = Schema.from_file()
        sample_json_path = posixpath.join(
            get_schema_path(*self.test_schema_version), "sample_data", "alert.json",
        )
        with open(sample_json_path, "r") as f:
            self.sample_alert = json.load(f)

    def _mock_alerts(self, n):
        """Return a list of alerts with mock values, matching
        self.sample_alert.
        """
        alerts = []
        for i in range(n):
            alert = self.sample_alert.copy()
            alert["alertId"] = i
            alerts.append(alert)
        return alerts

    def assert_alert_lists_equal(self, have_alerts, want_alerts):
        """Assert that two lists of mock alerts are equal - or at least,
        equal enough.

        We can't naively do `self.assertEqual(have_alerts, want_alerts)`
        because fastavro will explicitly populate an alert with "None" for
        every optional field when deserializing it. The sample alert.json files
        don't have those explicit Nones, and constructing them automatically
        seems complex.

        A simple check is just that the two lists have the same length and that
        the diaSourceIds match. diaSourceId is the only field that differs
        in a batch of mock data created with self._mock_alerts, so this is
        probably sufficient.
        """
        self.assertEqual(len(have_alerts), len(want_alerts))
        for i in range(len(have_alerts)):
            self.assertEqual(
                have_alerts[i]["alertId"], want_alerts[i]["alertId"],
                f"alert idx={i} has mismatched IDs",
            )

    @contextmanager
    def _temp_alert_file(self, alerts):
        with tempfile.TemporaryFile(mode="w+b") as alert_file:
            self.test_schema.store_alerts(alert_file, alerts)
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
        alerts = self._mock_alerts(5)
        with self._temp_alert_file(alerts) as alert_file:
            have_schema, have_alerts_iterable = retrieve_alerts(alert_file, self.test_schema)
            have_alerts = list(have_alerts_iterable)

        self.assert_alert_lists_equal(alerts, list(have_alerts))

        fastavro_keys = list(self.test_schema.definition.keys())
        for key in fastavro_keys:
            if '__' in key and '__len__' not in key:
                self.test_schema.definition.pop(key)

        self.assertEqual(self.test_schema.definition, have_schema.definition)

    def test_alert_file_with_one_alert(self):
        """Write a single alert to a file. It should be readable back out.
        """
        alerts = self._mock_alerts(1)

        with self._temp_alert_file(alerts) as alert_file:
            have_schema, have_alerts_iterable = retrieve_alerts(alert_file, self.test_schema)
            have_alerts = list(have_alerts_iterable)

        self.assert_alert_lists_equal(alerts, list(have_alerts))

        fastavro_keys = list(self.test_schema.definition.keys())
        for key in fastavro_keys:
            if '__' in key and '__len__' not in key:
                self.test_schema.definition.pop(key)

        self.assertEqual(self.test_schema.definition, have_schema.definition)

    def test_alert_file_with_no_alerts(self):
        """Write an alert file that contains no alerts at all. It should be
        readable.
        """
        alerts = []

        with self._temp_alert_file(alerts) as alert_file:
            have_schema, have_alerts_iterable = retrieve_alerts(alert_file, self.test_schema)
            have_alerts = list(have_alerts_iterable)

        self.assert_alert_lists_equal(alerts, list(have_alerts))

        fastavro_keys = list(self.test_schema.definition.keys())
        for key in fastavro_keys:
            if '__' in key and '__len__' not in key:
                self.test_schema.definition.pop(key)

        self.assertEqual(self.test_schema.definition, have_schema.definition)
