#!/usr/bin/env python
import json
import lsst.alert.packet_v2 as packet

with open("alert.json", "r") as f:
    data = json.load(f)

schema = packet.load_schema()
with open("fakeAlert.avro", "wb") as f:
    schema.store_alerts(f, [data])
