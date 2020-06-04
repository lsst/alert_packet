#!/usr/bin/env python
#
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

import argparse

import fastavro

import lsst.alert.packet


def parse_args():
    parser = argparse.ArgumentParser()
    # Default value based on LSE-81
    parser.add_argument('--visits-per-year', type=int, default=1056//10,
                        help='Number of visits per year')
    parser.add_argument('--num-alerts', type=int, default=10,
                        help='Number of simulated alert packets to generate')
    parser.add_argument('output_filename', type=str,
                        help="File to which to write alerts")
    return parser.parse_args()


def main():
    args = parse_args()

    schema = lsst.alert.packet.Schema.from_file()
    arrayCount = {'prvDiaSources': args.visits_per_year,
                  'prvDiaForcedSources': args.visits_per_year//12,
                  'prvDiaNondetectionLimits': 0}
    alerts = [lsst.alert.packet.simulate_alert(schema.definition,
                                        keepNull=['ssObject'],
                                        arrayCount=arrayCount)
              for _ in range(args.num_alerts)]

    for alert in alerts:
        assert(schema.validate(alert))

    with open(args.output_filename, "wb") as f:
        schema.store_alerts(f, alerts)

    with open(args.output_filename, "rb") as f:
        writer_schema, loaded_alerts = schema.retrieve_alerts(f)

    assert(schema == writer_schema)
    for a1, a2 in zip(alerts, loaded_alerts):
        assert(a1 == a2)


if __name__ == '__main__':
    main()
