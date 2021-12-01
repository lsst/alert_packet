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
import json

import fastavro
import requests

import lsst.alert.packet


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--schema-registry-url",
        type=str,
        default="http://alert-schemas.localhost",
        help="URL of a Schema Registry service",
    )
    parser.add_argument(
        "--subject",
        type=str,
        default="alert_packet",
        help="Schema Registry subject name to use",
    )
    return parser.parse_args()


def load_latest_schema():
    schema = lsst.alert.packet.Schema.from_file()
    normalized_schema = fastavro.schema.to_parsing_canonical_form(schema.definition)
    return normalized_schema


def upload_schema(registry_url, subject, normalized_schema):
    confluent_schema = {"schema": normalized_schema, "schemaType": "AVRO"}
    payload = json.dumps(confluent_schema)
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    url = f"{registry_url}/subjects/{subject}/versions"
    print(f"uploading schema to {url}")
    response = requests.post(url=url, data=payload, headers=headers)
    response.raise_for_status()
    print(f"done, status={response.status_code}")
    print(f"response text={response.text}")


def main():
    args = parse_args()
    schema = load_latest_schema()
    upload_schema(
        args.schema_registry_url,
        subject=args.subject,
        normalized_schema=schema,
    )


if __name__ == "__main__":
    main()
