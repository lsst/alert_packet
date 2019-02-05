"""Provide a lookup table for alert schemata.
"""

import json
import os
import zlib

__all__ = ["SchemaRegistry"]

class SchemaRegistry(object):
    """A registry for alert schemata.

    When a schema is registered, the registry allocates it a “hash” value. We
    can subsequently retrieve the schema by either the hash or by a version
    number.
    """
    def __init__(self):
        self._version_to_hash = {}
        self._hash_to_schema = {}

    def register_schema(self, schema, version):
        """Register a new schema in the registry.

        If an existing schema has the same hash, it is replaced.

        If an existing schema has the same version, the new schema replaces
        the old one under that version, but the old schema can still be
        accessed by hash.

        Parameters
        ----------
        schema : `lsst.alert.packet.Schema`
            Alert schema to register.
        version : `str`
            Version of the schema being registered. By convention, this is of
            the form ``MAJOR.MINOR``, but any string can be used.

        Returns
        -------
        schema_hash : `int`
            The hash that has been allocated to the schema.
        """
        schema_hash = self.calculate_hash(schema)
        self._version_to_hash[version] = schema_hash
        self._hash_to_schema[schema_hash] = schema
        return schema_hash

    def get_hash(self, schema_hash):
        """Return the schema corresponding to the given hash.

        Paramters
        ---------
        schema_hash : `int`
            The hash by which to look up the schema.

        Returns
        -------
        schema : `lsst.alert.packet.Schema`
            The corresponding schema.
        """
        return self._hash_to_schema[schema_hash]

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
        return self._hash_to_schema[self._version_to_hash[version]]

    @staticmethod
    def calculate_hash(schema):
        """Calculate a hash for the given schema.

        Parameters
        ----------
        schema : `lsst.alert.packet.Schema`
            Schema to be hashed.

        Returns
        -------
        schema_hash : `int`
            The calculated hash.
        """
        # Significant risk of collisions with more than a few schemata;
        # CRC32 is ok for prototyping but isn't sensible in production.
        return zlib.crc32(json.dumps(schema.definition, sort_keys=True).encode('utf-8'))

    @classmethod
    def from_filesystem(cls, root=None, schema_root_file="lsst.alert.avsc"):
        """Populate a schema registry based on the filesystem.

        Walk the directory tree from the root provided, locating files named
        according to `schema_root_file`, and load the corresponding schemata
        into the registry.
        """
        from .schema import Schema
        from .schema import get_schema_root
        if not root:
            root = get_schema_root()
        registry = cls()
        for root, dirs, files in os.walk(root, followlinks=False):
            if schema_root_file in files:
                schema = Schema.from_file(os.path.join(root, schema_root_file))
                version = ".".join(root.split("/")[-2:])
                registry.register_schema(schema, version)
        return registry
