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

"""Routines for working with Avro schemas.
"""

import io
import os.path
import pkg_resources
import posixpath

import fastavro

__all__ = ["get_schema_root", "get_latest_schema_version", "get_schema_path",
           "Schema", "get_path_to_latest_schema"]


def get_schema_root():
    """Return the root of the directory within which schemas are stored.
    """
    return pkg_resources.resource_filename(__name__, "schema")


def get_latest_schema_version():
    """Get the latest schema version.

    Returns
    -------
    major : `int`
        The major version number.
    minor : `int`
        The minor version number.

    """
    val = pkg_resources.resource_string(__name__, "schema/latest.txt")
    clean = val.strip()
    major, minor = clean.split(b".", 1)
    return int(major), int(minor)


def get_schema_path(major, minor):
    """Get the path to a package resource directory housing alert schema
    definitions.

    Parameters
    ----------
    major : `int`
        Major version number for the schema.
    minor : `int`
        Minor version number for the schema.

    Returns
    -------
    path : `str`
        Path to the directory containing the schemas.

    """

    # Note that posixpath is right here, not os.path, since pkg_resources
    # always uses slash-delimited paths, even on Windows.
    path = posixpath.join("schema", str(major), str(minor))
    return pkg_resources.resource_filename(__name__, path)


def get_path_to_latest_schema():
    """Get the path to the primary schema file for the latest schema.

    Returns
    -------
    path : `str`
        Path to the latest primary schema file.
    """

    major, minor = get_latest_schema_version()
    schema_path = get_schema_path(major, minor)
    return posixpath.join(schema_path, f"lsst.v{major}_{minor}.alert.avsc")


def resolve_schema_definition(to_resolve, seen_names=None):
    """Fully resolve complex types within a schema definition.

    That is, if this schema is defined in terms of complex types,
    substitute the definitions of those types into the returned copy.

    Parameters
    ----------
    schema : `list`
        The output of `fastavro.schema.load_schema`.

    Returns
    -------
    resolved_schema : `dict`
        The fully-resolved schema definition.

    Notes
    -----
    The schema is resolved in terms of the types which have been parsed
    and stored by fastavro (ie, are found in
    `fastavro.schema._schema.SCHEMA_DEFS`).

    The resolved schemas are supplied with full names and no namespace
    (ie, names of the form ``full.namespace.name``, rather than a
    namespace of ``full.namespace`` and a name of ``name``).
    """
    schema_defs = fastavro.schema._schema.SCHEMA_DEFS

    # Names of records, enums, and fixeds can only be used once in the
    # expanded schema. We'll re-use, rather than re-defining, names we have
    # previously seen.
    seen_names = seen_names or set()

    if isinstance(to_resolve, dict):
        # Is this a record, enum, or fixed that we've already seen?
        # If so, we return its name as a string and do not resolve further.
        if to_resolve['type'] in ('record', 'enum', 'fixed'):
            if to_resolve['name'] in seen_names:
                return to_resolve['name']
            else:
                seen_names.add(to_resolve['name'])
        output = {}
        for k, v in to_resolve.items():
            if k == "__fastavro_parsed":
                continue
            elif isinstance(v, list) or isinstance(v, dict):
                output[k] = resolve_schema_definition(v, seen_names)
            elif v in schema_defs and k != "name":
                output[k] = resolve_schema_definition(schema_defs[v],
                                                      seen_names)
            else:
                output[k] = v
    elif isinstance(to_resolve, list):
        output = []
        for v in to_resolve:
            if isinstance(v, list) or isinstance(v, dict):
                output.append(resolve_schema_definition(v, seen_names))
            elif v in schema_defs:
                output.append(resolve_schema_definition(schema_defs[v],
                                                        seen_names))
            else:
                output.append(v)
    else:
        raise Exception("Failed to parse.")

    return output


class Schema(object):
    """An Avro schema.

    Parameters
    ----------
    schema_definition : `dict`
        An Avro schema definition as returned by e.g.
        `fastavro.schema.load_schema`.

    Notes
    -----
    The interaction with `fastavro` here needs some explanation.

    When `fastavro` loads a schema, it parses each of the types contained
    within that schema and remembers them for future use. So that if, for
    example, your schema defines a type ``lsst.alert.diaSource``, `fastavro`
    will remember that type and use it when referring to your schema.

    However, it uses a single lookup table by type for these. Thus, if you
    load another schema which defines an ``lsst.alert.diaSource`` type which
    is not the same as the first, then it will clobber the earlier definition,
    and confusion will reign.

    We avoid this here by fully resolving everything (ie, all schemas are
    defined in terms of primitive types) and then clearing the `fastavro`
    cache after loading.
    """
    def __init__(self, schema_definition):
        self.definition = resolve_schema_definition(schema_definition)

    def serialize(self, record):
        """Create an Avro representation of data following this schema.

        Parameters
        ----------
        record : `dict`
            The data to be serialized to Avro.

        Returns
        -------
        avro_data : `bytes`
            An Avro serialization of the input data.
        """
        bytes_io = io.BytesIO()
        fastavro.schemaless_writer(bytes_io, self.definition, record)
        return bytes_io.getvalue()

    def deserialize(self, record):
        """Deserialize an Avro packet folowing this schema.

        Parameters
        ----------
        record : `bytes`
            The data to be deserialized.

        Returns
        -------
        alert_data : `dict`
            Deserialized packet contents.
        """
        bytes_io = io.BytesIO(record)
        message = fastavro.schemaless_reader(bytes_io, self.definition)
        return message

    def validate(self, record):
        """Validate packet contents against this schema.

        Parameters
        ----------
        record : `dict`
            The data to be checked for schema compliance.

        Returns
        -------
        valid : `bool`
            Whether or not the data complies with the schema.
        """
        fastavro.parse_schema(self.definition)
        return fastavro.validate(record, self.definition)

    def store_alerts(self, fp, records):
        """Store alert packets to the given I/O stream.

        Parameters
        ----------
        fp : derivative of `IOBase`
            I/O stream to which data will be written.
        records : iterable of `dict`
            Alert records to be stored.
        """
        fastavro.writer(fp, self.definition, records)

    def retrieve_alerts(self, fp):
        """Read alert packets from the given I/O stream.

        Parameters
        ----------
        fp : derivative of `IOBase`
            I/O stream from which data will be read.
        schema : `list`, optional
            A schema describing the contents of the Avro packets. If not
            provided, the schema used when writing the alert stream will be
            used.

        Returns
        -------
        schema : `lsst.alert.Schema`
            The schema with which alerts were written (which may be different
            from this schema being used for deserialization).
        records : iterable of `dict`
            Alert records.
        """
        from .io import retrieve_alerts
        schema, records = retrieve_alerts(fp, reader_schema=self)
        return schema, records

    def __eq__(self, other):
        """Compare schemas for equality.

        Schemas are regarded as equal if their fully-resolved definitions are
        the same.
        """
        return self.definition == other.definition

    @classmethod
    def from_file(cls, filename=None, root_name="lsst.v3_0.alert"):
        """Instantiate a `Schema` by reading its definition from the filesystem.

        Parameters
        ----------
        filename : `str`, optional
            Path to the schema root. Will recursively load referenced schemas,
            assuming they can be found; otherwise, will raise. If `None` (the
            default), will load the latest schema defined in this package.
        root_name : `str`, optional
            Name of the root of the alert schema.
        """
        if filename is None:
            major, minor = get_latest_schema_version()
            filename = os.path.join(
                get_schema_path(major, minor),
                root_name + ".avsc",
            )
        schema_definition = fastavro.schema.load_schema(filename)
        # fastavro gives a back a list if it recursively loaded more than one
        # file, otherwise a dict.
        if isinstance(schema_definition, dict):
            schema_definition = [schema_definition]

        schema_definition = next(schema for schema in schema_definition
                                 if schema['name'] == root_name)
        return cls(schema_definition)
