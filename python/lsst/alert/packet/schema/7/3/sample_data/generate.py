#!/usr/bin/env python
import json
import lsst.alert.packet as packet
from datetime import datetime
import pandas as pd

with open("alert.json", "r") as f:
    data = json.load(f)

    data_time = datetime.now()

    data['diaSource']['time_processed'] = pd.Timestamp(data_time)
    data['diaObject']['validityStart'] = pd.Timestamp(data_time)

schema = packet.SchemaRegistry.from_filesystem(schema_root="lsst.v7_1.alert").get_by_version("7.1")
with open("fakeAlert.avro", "wb") as f:
    schema.store_alerts(f, [data])
