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

Adding a new schema
-------------------

Steps to update the alert schema (for example, when the APDB schema is updated).

* Decide what the new schema version will be, following the guidelines given in `DMTN-093 <https://dmtn-093.lsst.io/#management-and-evolution>`_, referring to the current version number directories in ``python/lsst/alert/packet/schema``.
* ``setup -r .`` in this package's root.
* Checkout the ticket branch for your schema changes.
* Update the default ``schema_root`` kwarg in ``python/lsst/alert/packet/schemaRegistry.py:from_filesystem()`` to your new schema version number.

   * New schemas are built from the apdb. Any changes to a field should be done in apdb.yaml, and any changes to what is included/excluded should be made in updateSchema.py.
   * To update the schema, you must have the path to the apdb.yaml file and have chosen a version number. If the directory for your version
   * does not already exist, ``updateSchema.py`` will create it.

      * run ``python updateSchema.py /path/to/LSST/code/sdm_schemas/yml/apdb.yaml "6.0"`` All Generated files do not need to be altered.
      * Navigate to the new schema. Copy in the previous ``lsst.vX_X.avsc`` file and ``lsst.vX_X.diaNondetectionLimit.avsc``.
      * Within the two copied files, update ``"namespace": "lsst.vX_X",`` line at the top of each ``*.avsc`` file to the new version.
      * Update the contents of those avro schema files to reflect the new schema.
      * Update the sample alert packet in ``sample_data``:

         * Update ``alert.json`` to reflect the new schema.
         * Change the ``schema_root`` and ``get_by_version`` parameters in ``generate.py`` to your new version number.
         * Run ``python generate.py`` to produce a new ``fakeAlert.avro`` file with data filled in from the updated json file above and using the new schema files you made earlier.

   * Update the files ``*.avsc`` and ``*.json`` files in ``examples/`` to reflect the new schema.
   * Update the contents of ``latest.txt`` to your new schema version number.

* Add all of your new and updated files to ``git``, commit them with a message that includes the new schema version number and why it was incremented, and push your new branch.
* Test your changes with ap_association and ap_verify (these steps may be more involved if your ticket branch impacts multiple packets):

   * Clone a local copy of `ap_association <https://github.com/lsst/ap_association/>`_ and cd into that directory.
   * Setup ap_association with ``setup -kr .`` (``-k`` to "keep" your previously setup ``alert_packet``).
   * Confirm that your modified alert_packet is still setup: ``eups list -s alert_packet`` should show a ``LOCAL:`` directory being setup.
   * Run ``scons`` to confirm that your updated schema works with the ``ap_association`` tests.
   * Run at least one of the `ap_verify datasets <https://pipelines.lsst.io/v/daily/modules/lsst.ap.verify/running.html>`_ to confirm that your new alert schema works with the broader tests in ap_verify.

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
