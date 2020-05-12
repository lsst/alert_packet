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

    Raises
    ------
    RuntimeError
        Raised if alert data could not be read.
    """
    try:
        reader = fastavro.reader(fp, reader_schema=reader_schema.definition if reader_schema else None)
    except Exception as e:
        raise RuntimeError(f"failed to find alert data in "
                           f"{fp.name if hasattr(fp, 'name') else 'stream'}") from e
    records = [rec for rec in reader]
    return Schema(reader.writer_schema), records
