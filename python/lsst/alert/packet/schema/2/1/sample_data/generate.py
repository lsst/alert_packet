#!/usr/bin/env python
import json
import lsst.alert.packet as packet

with open("alert.json", "r") as f:
    data = json.load(f)

schema = packet.SchemaRegistry.from_filesystem().get_by_version("2.1")
with open("fakeAlert.avro", "wb") as f:
    schema.store_alerts(f, [data])
