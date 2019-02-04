"""Utilities for working with LSST Avro alerts.
"""

import io
import os.path

import fastavro

__all__ = ["get_schema_root", "load_schema", "write_avro_data",
           "read_avro_data", "load_stamp", "resolve_schema"]

def get_schema_root():
    """Return the root of the directory within which schemata are stored."""
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../schema"))

def load_schema(filename=None):
    """Load an Avro schema, potentially spread over muliple files.

    Parameters
    ----------
    filename : `str`, optional
        Path to the schema root. Will recursively load referenced schemas,
        assuming they can be found; otherwise, will raise. If `None` (the
        default), will load the latest schema defined in this package.

    Returns
    -------
    schema : `dict`
        Parsed schema information.

    Todo
    ----
    Should take a `version` argument (instead of? as well as?) a filename, and
    return the corresponding schema.
    """
    if filename is None:
        filename = os.path.join(get_schema_root(), "latest", "lsst.alert.avsc")
    return fastavro.schema.load_schema(filename)

def load_stamp(file_path):
    """Load a cutout postage stamp file to include in alert.
    """
    _, fileoutname = os.path.split(file_path)
    with open(file_path, mode='rb') as f:
        cutout_data = f.read()
        cutout_dict = {"fileName": fileoutname, "stampData": cutout_data}
    return cutout_dict

def write_avro_data(data, schema):
    """Create an Avro representation of data following a given schema.

    Parameters
    ----------
    data : `dict`
        The data to be serialized to Avro.
    schema : `dict`
        The schema according to which the data will be written.

    Returns
    -------
    avro_data : `bytes`
        An Avro serialization of the input data.
    """
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, data)
    return bytes_io.getvalue()

def read_avro_data(data, schema):
    """Deserialize an Avro packet.

    Parameters
    ----------
    data : `bytes`
        The data to be deserialized.
    schema : `dict`
        A schema describing the Avro packet.

    Returns
    -------
    alert_data : `dict`
        Deserialized packet contents.
    """
    bytes_io = io.BytesIO(data)
    message = fastavro.schemaless_reader(bytes_io, schema)
    return message

def resolve_schema(schema, root_name='lsst.alert'):
    """Expand nested types within a schema.

    That is, if one type is given in terms of some other type, substitute the
    definition of the second type into the definition of the first.

    Parameters
    ----------
    schema : `list` of `dict`
        Schema as returned by `~lsst.alert.load_schema`. Each type definition
        is described by a separate element in the list.
    root_name : `str`
        Name of the root element of the resulting schema. The contituents of
        this are what will be expanded.

    Output
    ------
    expanded_schema : `dict`
        A single dictionary describing the expanded schema.
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

    schema_types = {entry['name'] : entry for entry in schema}
    schema_root = schema_types.pop('lsst.alert')
    return expand_types(schema_root, schema_types)
