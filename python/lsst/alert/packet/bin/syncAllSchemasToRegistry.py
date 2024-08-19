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
import re
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
        default="alert-packet",
        help="Schema Registry subject name to use",
    )
    return parser.parse_args()


def upload_schema(registry_url, subject, schema_registry):
    """Parse schema registry and upload all schemas.
    """
    for version in schema_registry.known_versions:
        schema = schema_registry.get_by_version(version)
        numbers = re.findall(r'\d+', version)
        numbers[1] = str(numbers[1]).zfill(2)
        version_number = int(''.join(numbers))
        normalized_schema = fastavro.schema.to_parsing_canonical_form(
            schema.definition)
        confluent_schema = {"version": version_number,
                            "id": version_number, "schema": normalized_schema}
        payload = json.dumps(confluent_schema)
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        url = f"{registry_url}/subjects/{subject}/versions"
        print(f"uploading schema to {url}")
        response = requests.post(url=url, data=payload, headers=headers)
        # response.raise_for_status()
        print(f"done, status={response.status_code}")
        print(f"response text={response.text}")


def delete_schema(registry_url, subject):
    """Delete schema and then remake it in import mode"""
    # Define the URLs
    url_mode = f"{registry_url}/mode/{subject}"
    url_schemas = f"{registry_url}/subjects/{subject}"
    url_schema_versions = f"{registry_url}/subjects/{subject}/versions"
    response = requests.get(url_schema_versions)

    # Schema registry must be empty to put it in import mode. If it exists,
    # remove it and remkae the schema. If not, continue.
    if response.status_code == 200:
        print('The schema will be deleted and remade in import mode.')
        response = requests.delete(url_schemas)
        print('Status Code:', response.status_code)
        print('Response Text:', response.text)
    else:
        print('The schema does not exist. Creating in import mode.')

    # Switch registry to import mode.
    headers = {
        'Content-Type': 'application/json'
    }

    # Define the data to send
    data = {
        'mode': 'IMPORT'
    }

    # Perform the PUT request
    response = requests.put(url_mode, headers=headers, data=json.dumps(data))

    # Check the status code and response
    print('Status Code:', response.status_code)
    print('Response Text:', response.text)


def close_schema(registry_url, subject):
    """Return the schema registry from import mode to readwrite.
    """
    data = {
        "mode": "READWRITE"
    }

    # Headers to specify the content type
    headers = {
        'Content-Type': 'application/json'
    }

    url_mode = f"{registry_url}/mode/{subject}"
    # Send the PUT request
    response = requests.put(url_mode, json=data, headers=headers)
    print(f'Status Code: {response.status_code}')
    print(f'Response Text: {response.text}')


def main():
    args = parse_args()
    delete_schema(args.schema_registry_url,args.subject)
    schema_registry = lsst.alert.packet.schemaRegistry.SchemaRegistry().all_schemas_from_filesystem()
    upload_schema(
        args.schema_registry_url,
        subject=args.subject,
        schema_registry=schema_registry
    )
    close_schema(args.schema_registry_url, args.subject)


if __name__ == "__main__":
    main()
