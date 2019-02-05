"""Routines for working with Avro schemata.
"""

import io
import os.path

import fastavro

__all__ = ["get_schema_root", "Schema"]

def get_schema_root():
    """Return the root of the directory within which schemata are stored."""
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../schema"))

class Schema(object):
    """An Avro schema.

    Parameters
    ----------
    schema_definition : `dict` or `list`
        An Avro schema definition as returned by e.g.
        `fastavro.schema.load_schema`.
    root_name : `str`, optional
        The root element of the schema.
    """
    def __init__(self, schema_definition, root_name="lsst.alert"):
        self.definition = schema_definition
        self.root_name = root_name

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
        """Compare `Schema`s for equality.

        Schemas are regarded as equal if their fully-resolved definitions are
        the same.
        """
        return self.resolved.definition == other.resolved.definition

    @property
    def resolved(self):
        """Return a copy of this Schema with nested types resolved.

        That is, if this schema is defined in terms of complex types,
        substitute the definitions of those types into the returned copy.

        Returns
        -------
        resolved_schema : `lsst.alert.Schema`
            The fully-resolved schema.
        """
        def expand_types(input_data, data_types):
            """Recursively substitute `data_types` into `input_data`.
            """
            if isinstance(input_data, dict):
                output = {}
                for k, v in input_data.items():
                    if k == "__fastavro_parsed":
                        continue
                    elif isinstance(v, list) or isinstance(v, dict):
                        output[k] = expand_types(v, data_types)
                    elif v in data_types.keys():
                        output[k] = data_types[v]
                    else:
                        output[k] = v
            elif isinstance(input_data, list):
                output = []
                for v in input_data:
                    if isinstance(v, list) or isinstance(v, dict):
                        output.append(expand_types(v, data_types))
                    elif v in data_types.keys():
                        output.append(data_types[v])
                    else:
                        output.append(v)
            else:
                raise Exception("Failed to parse.")

            return output

        schema_types = {entry['name'] : entry for entry in self.definition}
        schema_root = schema_types.pop('lsst.alert')
        return Schema(expand_types(schema_root, schema_types), root_name=self.root_name)

    @classmethod
    def from_file(cls, filename=None, root_name="lsst.alert"):
        """Instantiate a `Schema` by reading its definition from the filesystem.

        Parameters
        ----------
        filename : `str`, optional
            Path to the schema root. Will recursively load referenced schemas,
            assuming they can be found; otherwise, will raise. If `None` (the
            default), will load the latest schema defined in this package.
        root_name : `str`, optional
            The name of the root element of the schema.
        """
        if filename is None:
            filename = os.path.join(get_schema_root(), "latest", "lsst.alert.avsc")
        return cls(fastavro.schema.load_schema(filename), root_name)
