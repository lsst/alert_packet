"""Routines for working with Avro schemas.
"""

import io
import os.path

import fastavro

__all__ = ["get_schema_root", "Schema"]

def get_schema_root():
    """Return the root of the directory within which schemas are stored.
    """
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../schema"))

class Schema(object):
    """An Avro schema.

    Parameters
    ----------
    schema_definition : `dict` or `list`
        An Avro schema definition as returned by e.g.
        `fastavro.schema.load_schema`.
    """
    def __init__(self, schema_definition):
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
        return retrieve_alerts(fp, reader_schema=self)

    def __eq__(self, other):
        """Compare schemas for equality.

        Schemas are regarded as equal if their fully-resolved definitions are
        the same.
        """
        return self.definition == other.definition

    @classmethod
    def resolve(cls, to_resolve):
        """Fully resolve complex types within a schema.

        That is, if this schema is defined in terms of complex types,
        substitute the definitions of those types into the returned copy.

        Parameters
        ----------
        schema : `list`
            The output of `fastavro.schema.load_schema`.

        Returns
        -------
        resolved_schema : `dict`
            The fully-resolved schema.

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

        if isinstance(to_resolve, dict):
            output = {}
            for k, v in to_resolve.items():
                if k == "__fastavro_parsed":
                    continue
                elif isinstance(v, list) or isinstance(v, dict):
                    output[k] = Schema.resolve(v)
                elif v in schema_defs and k != "name":
                    output[k] = Schema.resolve(schema_defs[v])
                else:
                    output[k] = v
        elif isinstance(to_resolve, list):
            output = []
            for v in to_resolve:
                if isinstance(v, list) or isinstance(v, dict):
                    output.append(Schema.resolve(v))
                elif v in schema_defs:
                    output.append(cls.resolve(schema_defs[v]))
                else:
                    output.append(v)
        else:
            raise Exception("Failed to parse.")

        return output

    @classmethod
    def from_file(cls, filename=None):
        """Instantiate a `Schema` by reading its definition from the filesystem.

        Parameters
        ----------
        filename : `str`, optional
            Path to the schema root. Will recursively load referenced schemas,
            assuming they can be found; otherwise, will raise. If `None` (the
            default), will load the latest schema defined in this package.

        Notes
        -----
        The interaction with `fastavro` here needs some explanation.

        When `fastavro` loads a schema, it parses each of the types contained
        within that schema and remembers them for future use. So that if, for
        example, your schema defines a type ``lsst.alert.diaSource``,
        `fastavro` will remember that type and use it when referring to your
        schema.

        However, it uses a single lookup table by type for these. Thus, if you
        load another schema which defines an ``lsst.alert.diaSource`` type
        which is not the same as the first, then it will clobber the earlier
        definition, and confusion will reign.

        We avoid this here by fully resolving everything (ie, all schemas are
        defined in terms of primitive types) and then clearing the `fastavro`
        cache after loading.
        """
        if filename is None:
            filename = os.path.join(get_schema_root(), "latest", "lsst.alert.avsc")
        initial_schema = fastavro.schema.load_schema(filename)
        resolved_schema = cls.resolve(next(schema for schema in initial_schema
                                           if schema['name'] == 'lsst.alert'))
        fastavro.schema._schema.SCHEMA_DEFS.clear()
        return cls(resolved_schema)
