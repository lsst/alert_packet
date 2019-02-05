"""Routines for working with Avro schemata.
"""

import os.path

import fastavro

__all__ = ["get_schema_root", "load_schema", "resolve_schema"]

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
    schema : `list`
        Parsed schema information.

    Todo
    ----
    Should take a `version` argument (instead of? as well as?) a filename, and
    return the corresponding schema.
    """
    if filename is None:
        filename = os.path.join(get_schema_root(), "latest", "lsst.alert.avsc")
    return fastavro.schema.load_schema(filename)

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
