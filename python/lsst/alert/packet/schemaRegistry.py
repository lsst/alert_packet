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

"""Provide a lookup table for alert schemas.
"""

import json
import os
import zlib

__all__ = ["SchemaRegistry"]

class SchemaRegistry(object):
    """A registry for alert schemas.

    When a schema is registered, the registry allocates it an ID. We can
    subsequently retrieve the schema by either the ID or by a version number.
    """
    def __init__(self):
        self._version_to_id = {}
        self._id_to_schema = {}

    def register_schema(self, schema, version):
        """Register a new schema in the registry.

        If an existing schema has the same ID, it is replaced.

        If an existing schema has the same version, the new schema replaces
        the old one under that version, but the old schema can still be
        accessed by ID.

        Parameters
        ----------
        schema : `lsst.alert.packet.Schema`
            Alert schema to register.
        version : `str`
            Version of the schema being registered. By convention, this is of
            the form ``MAJOR.MINOR``, but any string can be used.

        Returns
        -------
        schema_id : `int`
            The ID that has been allocated to the schema.
        """
        schema_id = self.calculate_id(schema)
        self._version_to_id[version] = schema_id
        self._id_to_schema[schema_id] = schema
        return schema_id

    def get_by_id(self, schema_id):
        """Return the schema corresponding to the given ID.

        Paramters
        ---------
        schema_id : `int`
            The ID by which to look up the schema.

        Returns
        -------
        schema : `lsst.alert.packet.Schema`
            The corresponding schema.
        """
        return self._id_to_schema[schema_id]

    def get_by_version(self, version):
        """Return the schema corresponding to the given version.

        Paramters
        ---------
        version : `str`
            The version by which to look up the schema. Corresponds to the
            version specified when the schema was registered.

        Returns
        -------
        schema : `lsst.alert.packet.Schema`
            The corresponding schema.
        """
        return self._id_to_schema[self._version_to_id[version]]

    @property
    def known_versions(self):
        """Return all the schema versions tracked by this registry.

        Returns
        -------
        schemas : `set` of `str`
            Set of schema versions.
        """
        return set(self._version_to_id)

    @staticmethod
    def calculate_id(schema):
        """Calculate an ID for the given schema.

        Parameters
        ----------
        schema : `lsst.alert.packet.Schema`
            Schema for which an ID will be derived.

        Returns
        -------
        schema_id : `int`
            The calculated ID.
        """
        # Significant risk of collisions with more than a few schemas;
        # CRC32 is ok for prototyping but isn't sensible in production.
        return zlib.crc32(json.dumps(schema.definition,
                                     sort_keys=True).encode('utf-8'))

    @classmethod
    def from_filesystem(cls, root=None, schema_root="lsst.v3_0.alert"):
        """Populate a schema registry based on the filesystem.

        Walk the directory tree from the root provided, locating files named
        according to `schema_root_file`, and load the corresponding schemas
        into the registry.
        """
        from .schema import Schema
        from .schema import get_schema_root
        if not root:
            root = get_schema_root()
        registry = cls()
        schema_root_file = schema_root + ".avsc"
        for root, dirs, files in os.walk(root, followlinks=False):
            if schema_root_file in files:
                schema = Schema.from_file(os.path.join(root, schema_root_file),
                                          root_name=schema_root)
                version = ".".join(root.split("/")[-2:])
                registry.register_schema(schema, version)
        return registry
