"""Routines for loading data from files.
"""

import os.path

import fastavro

from .schema import Schema

__all__ = ["load_stamp", "retrieve_alerts"]

def load_stamp(file_path):
    """Load a cutout postage stamp file to include in alert.
    """
    _, fileoutname = os.path.split(file_path)
    with open(file_path, mode='rb') as f:
        cutout_data = f.read()
        cutout_dict = {"fileName": fileoutname, "stampData": cutout_data}
    return cutout_dict

def retrieve_alerts(fp, reader_schema=None):
    """Read alert packets from the given I/O stream.

    Parameters
    ----------
    fp : derivative of `IOBase`
        I/O stream from which data will be read.
    reader_schema : `dict` or `list`, optional
        A schema describing the contents of the Avro packets. If not provided,
        the schema used when writing the alert stream will be used.

    Returns
    -------
    schema : `lsst.alert.Schema`
        The schema with which alerts were written (which may be different from
        the schema being used for deserialization).
    records : iterable of `dict`
        Alert records.
    """
    reader = fastavro.reader(fp, reader_schema=reader_schema.definition if reader_schema else None)
    records = [rec for rec in reader]
    return Schema(reader.writer_schema), records
