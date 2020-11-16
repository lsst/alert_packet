#!/usr/bin/env python
import json
import lsst.alert.packet as packet

with open("alert.json", "r") as f:
    data = json.load(f)

schema = packet.SchemaRegistry.from_filesystem(schema_root="lsst.v4_0.alert").get_by_version("4.0")
with open("fakeAlert.avro", "wb") as f:
    schema.store_alerts(f, [data])
