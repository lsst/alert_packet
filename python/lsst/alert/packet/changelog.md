lsst.alert.packet.schema
========================

lsst.alert.packet no longer stores multiple schema versions. It always operates
using the latest schema version when writing data. When reading data, it always
trusts the file's schema.

- A new variable, `alert_schema` is added. This holds the (parsed, validated)
  current Avro schema for an alert packet as a dictionary.
- The Schema class is removed. Its methods (`store_alerts`, `serialize`,
  `deserialize`) are moved to be top-level functions in the
  `lsst.alert.packet.io` module, and they operate using the latest schema
  version.
- get_latest_schema_version is removed.
- get_schema_path is removed.
- get_path_to_latest_schema is removed.
- resolve_schema_definitions is removed.
- get_schema_root is removed.



lsst.alert.packet.io
====================

The `retrieve_alerts` function previously returned the writer schema, and
alerts. Now it only returns alerts.

The optional reader_schema parameter for the `retrieve_alerts` function should
now be any Avro schema object, rather than one wrapped with
`lsst.alert.packet.schema.Schema`. This matches the existing documentation for
the function.

A new `store_alerts` function is added. It takes a file-like I/O stream and an
iterable of records; it writes the records to the file using the current alert
schema.

A new `serialize` function is added. It takes a single alert record. It emits
the `bytes` that represent serialization of that alert record with the current
schema.

A new `deserialize` function is added. It takes encoded `bytes`. It emits a
deserialized alert record, trusting the bytes to represent serialization with
the current schema.


lsst.alert.packet.schemaRegistry
================================

This module is removed, since multiple schemas are no longer stored.

Avro Schemas
============

Just one version of the Avro schema is stored, so the path structure is
reorganized, from `lsst/alert/packet/schema/4/0/lsst.v4_0.alert.avsc` to
`lsst/alert/packet/schema/lsst.alert.avsc`.

The `latest.txt` file is removed.
