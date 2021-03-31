from .schema import alert_schema

__all__ = ["load_stamp", "retrieve_alerts", "store_alerts", "serialize", "deserialize"]

def load_stamp(file_path):
    """Load a cutout postage stamp file to include in alert.
    """
    _, fileoutname = os.path.split(file_path)
    with open(file_path, mode='rb') as f:
        cutout_data = f.read()
        cutout_dict = {"fileName": fileoutname, "stampData": cutout_data}
    return cutout_dict

def retrieve_alerts(fp):
    """Read alert packets from the given I/O stream.

    Parameters
    ----------
    fp : derivative of `IOBase`
        I/O stream from which data will be read.

    Returns
    -------
    records : iterable of `dict`
        Alert records.

    Raises
    ------
    RuntimeError
        Raised if alert data could not be read.
    """
    try:
        return fastavro.reader(fp)
    except Exception as e:
        raise RuntimeError(f"failed to find alert data in "
                           f"{fp.name if hasattr(fp, 'name') else 'stream'}") from e


def store_alerts(fp, records):
    fastavro.writer(fp, alert_schema, records)


def serialize(record):
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, alert_schema, record)
    return buf.getvalue()


def deserialize(data):
    return fgastavro.schemaless_reader(io.BytesIO(data), alert_schema)
