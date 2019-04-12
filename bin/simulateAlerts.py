#!/usr/bin/env python

import argparse

import fastavro

import lsst.alert

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

if __name__ == '__main__':
    args = parse_args()

    schema = lsst.alert.Schema.from_file()
    arrayCount = {'prvDiaSources': args.visits_per_year,
                  'prvDiaForcedSources': args.visits_per_year//12,
                  'prvDiaNondetectionLimits': 0}
    alerts = [lsst.alert.simulate_alert(schema.resolved.definition,
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
