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

from __future__ import annotations

import io
import tempfile
from importlib import resources
from pathlib import PurePath
from lsst.resources import ResourcePath

import fastavro

__all__ = ["get_schema_root", "get_latest_schema_version", "get_schema_path",
           "Schema", "get_path_to_latest_schema", "get_schema_root_uri",
           "get_uri_to_latest_schema", "get_schema_uri"]


def _get_ref(*args):
    """Return the package resource file path object.

    Parameters are relative to lsst.alert.packet.
    """
    return resources.files("lsst.alert.packet").joinpath(*args)


def _get_dir_uri(*args: str) -> ResourcePath:
    """Return the package resource associated with the given directory
     components as a URI.

    Returns
    -------
    uri : `lsst.resources.ResourcePath`
        The URI derived from the supplied paths.
    """
    return ResourcePath("resource://lsst.alert.packet/" + "/".join(args),
                        forceDirectory=True)


def get_schema_root():
    """Return the root of the directory within which schemas are stored.

    This might be a temporary location if a zip distribution file is used.
    """
    return _get_ref("schema")


def get_schema_root_uri() -> ResourcePath:
    """Return the ``resource`` URI corresponding to the location where
    schemas are stored."""
    return _get_dir_uri("schema")


def get_latest_schema_version():
    """Get the latest schema version.

    Returns
    -------
    major : `int`
        The major version number.
    minor : `int`
        The minor version number.

    """
    with _get_ref("schema", "latest.txt").open("rb") as fh:
        val = fh.read()
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
    return _get_ref("schema", str(major), str(minor)).as_posix()


def get_schema_uri(major: int, minor: int) -> ResourcePath:
    """Get the URI to a package resource directory housing alert schema
    definitions.

    Parameters
    ----------
    major : `int`
        Major version number for the schema.
    minor : `int`
        Minor version number for the schema.

    Returns
    -------
    uri : `lsst.resources.ResourcePath`
        ``resource`` URI to the directory containing the schemas.
    """
    return _get_dir_uri("schema", str(major), str(minor))


def get_path_to_latest_schema():
    """Get the path to the primary schema file for the latest schema.

    Returns
    -------
    path : `str`
        Path to the latest primary schema file.
    """

    major, minor = get_latest_schema_version()
    schema_path = PurePath(get_schema_path(major, minor))
    return (schema_path / f"lsst.v{major}_{minor}.alert.avsc").as_posix()


def get_uri_to_latest_schema() -> ResourcePath:
    """Get the URI to to the primary file for the latest schema.

    Returns
    -------
    uri : `lsst.resources.ResourcePath`
        The ``resource`` URI to the latest schema.
    """
    major, minor = get_latest_schema_version()
    schema_uri = get_schema_uri(major, minor)
    return schema_uri.join(f"lsst.v{major}_{minor}.alert.avsc")


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
    This method is only needed for old fastavro (<=0.24).
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


class Schema:
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
        if hasattr(fastavro.schema._schema, 'SCHEMA_DEFS'):
            # Old fastavro
            self.definition = resolve_schema_definition(schema_definition)
        else:
            # New fastavro
            self.definition = schema_definition

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
    def from_uri(cls, base_uri: None | str | ResourcePath = None) -> Schema:
        """Instantiate a `Schema` by reading its definition from a URI.

        Parameters
        ----------
        base_uri : `str` or `lsst.resources.ResourcePath` or `None`
            URI to the base schema as either a `~lsst.resources.ResourcePath`
            or a string that can be converted to one. If `None` the most
            recent default schema will be used.
        """
        if base_uri is None:
            uri = get_uri_to_latest_schema()
        else:
            uri = ResourcePath(base_uri)

        if uri.isLocal:
            return cls.from_file(uri.ospath)

        # fastavro requires that the schema file is local and that all the
        # referenced schema files are also local. This means that for a remote
        # URI all related schema files must be downloaded. Additionally they
        # must all have the original names and not temporary names.

        # Special case resource URIs. If the package is installed in expanded
        # form the local file will have the original name, else if the package
        # is still in a wheel it will have a temporary name.
        if uri.scheme == "resource":
            with uri.as_local() as local_file:
                if local_file.basename() == uri.basename():
                    # Likely already a local file.
                    return cls.from_file(local_file.ospath)

        # This URI is a remote resource (eg S3) or a package resource in a
        # wheel. Need to scan the directory and download all .avsc files.
        uri_dir = uri.dirname()

        with tempfile.TemporaryDirectory() as tmpdir:
            tempdir_uri = ResourcePath(tmpdir, forceDirectory=True)
            for file in ResourcePath.findFileResources([uri_dir],
                                                       file_filter=f"\\{uri.getExtension()}$"):
                target = tempdir_uri.join(file.basename())
                target.transfer_from(file, transfer="copy")

            return cls.from_file(tempdir_uri.join(uri.basename()).ospath)

    @classmethod
    def from_file(cls, filename=None):
        """Instantiate a `Schema` by reading its definition from the
        filesystem.

        Parameters
        ----------
        filename : `str`, optional
            Path to the schema root (/path/to/lsst.vM_m.alert.avsc).
            Will recursively load referenced schemas, assuming they can be
            found; otherwise, will raise. If `None` (the
            default), will load the latest schema defined in this package.
        """
        if filename is None:
            filename = get_path_to_latest_schema()

        root_name = PurePath(filename).stem
        schema_definition = fastavro.schema.load_schema(filename)
        if hasattr(fastavro.schema._schema, 'SCHEMA_DEFS'):
            # Old fastavro gives a back a list if it recursively loaded more
            # than one file, otherwise a dict.
            if isinstance(schema_definition, dict):
                schema_definition = [schema_definition]

            schema_definition = next(schema for schema in schema_definition
                                     if schema['name'] == root_name)

        return cls(schema_definition)
