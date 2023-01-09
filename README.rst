#################
alert_packet
#################

This package provides information about, and utilties for working with, LSST alerts in `Apache Avro`_ format.
In particular, it includes:

- Alert schemas;
- Examples of alert contents;
- Utilities code for working with alerts.

Refer to `DMTN-093`_ for more information on the LSST alert format.

.. _Apache Avro: https://avro.apache.org
.. _DMTN-093: https://dmtn-093.lsst.io

Schemas
=======

Alert schemas are located in the ``schema`` directory.

Schemas are filed according to their version number, following a ``MAJOR.MINOR`` scheme.
We maintain ``FORWARD_TRANSITIVE`` compatibility within a major version, per the `Confluent compatibility model`_.
The latest version of the schema may always be found in ``schema/latest.txt``.

.. _Confluent compatibility model: https://docs.confluent.io/current/schema-registry/docs/avro.html#forward-compatibility

Example Alert Contents
======================

Example alert contents are provided in the ``sample_data`` directories included with the corresponding schema.

Utility Code
============

All code is written in Python, and uses the `fastavro`_ library.
Simulation code also requires `NumPy`_.
Both of these may be installed using standard tooling (pip, Conda, etc).

Although this package contains multiple versions of the alert schema, this library code is only written and tested using the latest version (``schema/latest``) at present.
Future versions of this package should offer wider compatibility.

Installation
------------

Using pip
^^^^^^^^^

The name of the package is `lsst-alert-packet`::

  $ pip install lsst-alert-packet

Using EUPS
^^^^^^^^^^

This package may be managed using `EUPS`_.
Assuming EUPS is available on your system, simply::

  $ git clone https://github.com/lsst/alert_packet.git
  $ setup -r alert_packet

.. _EUPS: https://github.com/RobertLuptonTheGood/eups/

Library
-------

The ``lsst.alert.packet`` Python package provides a suite of routines for working with alerts in the Avro format.

Command Line
------------

``validateAvroRoundTrip.py`` demonstrates round-tripping a simple alert through the Avro system.
Sample data is provided in the ``schema/latest/sample_data/alert.json`` file, or an alternative may be provided on the command line.
Optionally, the path to binary data files to be included in the packet as “postage stamp” images may be provided.
If the ``--print`` flag is given, the alert contents are printed to screen for sanity checking.

``simulateAlerts.py`` writes simulated alert packets to disk in Avro format.
The resultant data is schema compliant, but the simulations are not intended to be realistic: packets are populated with pseudorandom numbers.
The number of visits per year (equivalent to the number of previous DIASources observed for each alert) and the number of alerts to simulate may be specified on the command line.
Thus::

   $ simulateAlerts.py --visits-per-year=100 --num-alerts=10 ./output_file.avro

.. _fastavro: https://fastavro.readthedocs.io/en/latest/
.. _NumPy: http://www.numpy.org
