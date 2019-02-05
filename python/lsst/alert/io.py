"""Routines for serializing and storing Avro alert packets.
"""

import io
import os.path

import fastavro

__all__ = ["load_stamp", "serialize_alert", "deserialize_alert",
           "store_alerts", "retrieve_alerts"]

def load_stamp(file_path):
    """Load a cutout postage stamp file to include in alert.
    """
    _, fileoutname = os.path.split(file_path)
    with open(file_path, mode='rb') as f:
        cutout_data = f.read()
        cutout_dict = {"fileName": fileoutname, "stampData": cutout_data}
    return cutout_dict

def serialize_alert(schema, record):
    """Create an Avro representation of data following a given schema.

    Parameters
    ----------
    schema : `list`
        The schema according to which the data will be written.
    record : `dict`
        The data to be serialized to Avro.

    Returns
    -------
    avro_data : `bytes`
        An Avro serialization of the input data.
    """
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    return bytes_io.getvalue()

def deserialize_alert(schema, record):
    """Deserialize an Avro packet.

    Parameters
    ----------
    schema : `list`
        A schema describing the Avro packet.
    record : `bytes`
        The data to be deserialized.

    Returns
    -------
    alert_data : `dict`
        Deserialized packet contents.
    """
    bytes_io = io.BytesIO(record)
    message = fastavro.schemaless_reader(bytes_io, schema)
    return message

def store_alerts(fp, schema, records):
    """Store alert packets to the given stream.

    Parameters
    ----------
    fp : derivative of `IOBase`
        I/O stream to which data will be written.
    schema : `list`
        A schema describing the Avro packet.
    records : iterable of `dict`
        Alert records to be stored.

    Notes
    -----
    This is a thin wrapper around `fastavro.writer` for API consistency.
    """
    fastavro.writer(fp, schema, records)

def retrieve_alerts(fp, schema=None):
    """Read alert packets from the given stream.

    Parameters
    ----------
    fp : derivative of `IOBase`
        I/O stream to which data will be written.
    schema : `list`, optional
        A schema describing the contents of the Avro packets. If not provided,
        the schema used when writing the alert stream will be used.

    Returns
    -------
    schema : `list`
        The schema with which alerts were written (which may be different from
        the schema being used by the reader).
    records : iterable of `dict`
        Alert records.
    """
    reader = fastavro.reader(fp, reader_schema=schema)
    records = [rec for rec in reader]
    return reader.writer_schema, records
