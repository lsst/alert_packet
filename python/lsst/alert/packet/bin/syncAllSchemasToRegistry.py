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
        help="URL of a Confluent Schema Registry service",
    )
    parser.add_argument(
        "--subject",
        type=str,
        default="alert-packet",
        help="Schema Registry subject name to use",
    )
    return parser.parse_args()


def upload_schemas(registry_url, subject, schema_registry):
    """Parse schema registry and upload all schemas.
    """
    for schema_id in schema_registry.known_ids:
        schema = schema_registry.get_by_id(schema_id)
        normalized_schema = fastavro.schema.to_parsing_canonical_form(
            schema.definition)
        confluent_schema = {"version": schema_id,
                            "id": schema_id, "schema": normalized_schema}
        payload = json.dumps(confluent_schema)
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        url = f"{registry_url}/subjects/{subject}/versions"
        print(f"uploading schema to {url}")
        response = requests.post(url=url, data=payload, headers=headers)
        response.raise_for_status()
        print(f"done, status={response.status_code}")
        print(f"response text={response.text}")

        # Register in the TopicNameStrategy-compatible subject, needs to be in
        # 0.0 format.
        version = next(key for key, value in schema_registry._version_to_id.items() if
            value == schema_id)

        # Topic for TopicNameStrategy-compatible schema
        topic_subject = f"lsst-alerts-v{version}-value"
        url = f"{registry_url}/subjects/{topic_subject}/versions"
        print(f"uploading schema to {url}")
        print(f"Confluent Version is  {str(schema_id)}")

        # Since the schema is identical, the registry should reuse the ID automatically.
        payload_with_metadata = {
            "schema": normalized_schema,
            "metadata": {
                "properties": {
                    "confluent:version": str(schema_id)
                }
            },
        }
        try:
            json.dumps(payload_with_metadata)
            print("JSON encoding OK")
        except Exception as e:
            print("JSON encoding FAILED:", e)

        mode_resp = requests.get(f"{registry_url}/mode")
        print("Registry mode:", mode_resp.text)

        config_resp = requests.get(f"{registry_url}/config")
        print("Compatibility:", config_resp.text)

        response = requests.post(url=url, json=payload_with_metadata, headers=headers)
        if not response.ok:
            print("ERROR: Registry rejected request")


def clear_schema_registry_for_import(registry_url, subject):
    """Delete schemas in the registry and then remake it in import mode"""
    # Define the URLs
    url_mode = f"{registry_url}/mode/{subject}"

    # Get all subjects
    url_subjects = f"{registry_url}/subjects"
    response = requests.get(url_subjects)

    if response.status_code == 200:
        print('All schemas will be deleted to prepare for import mode.')
        for subj in response.json():
            print(f'Deleting schema subject: {subj}')
            url_subj = f"{registry_url}/subjects/{subj}"
            # Ignore 404s if already deleted
            requests.delete(url_subj)
            # Hard delete to ensure it's gone for IMPORT mode otherwise
            # it can error
            requests.delete(url_subj, params={"permanent": "true"})
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


def close_schema_registry(registry_url, subject):
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


def check_metadata(registry_url, subject, version):
    """Check if the metadata is correctly stored in the schema registry."""
    topic_subject = f"lsst-alerts-v{version}-value"
    # Check latest version, there should only be one.
    url = f"{registry_url}/subjects/{topic_subject}/versions/latest"
    
    print(f"Checking metadata at {url}")
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        print(json.dumps(data.get('metadata'), indent=2))
        
        metadata = data.get('metadata')
        if metadata and 'properties' in metadata and 'confluent:version' in metadata['properties']:
             print(f"SUCCESS: Found confluent:version = {metadata['properties']['confluent:version']}")
        else:
             print("FAILURE: Metadata or confluent:version not found.")
    else:
        print(f"Failed to fetch schema: {response.status_code} {response.text}")


def main():
    args = parse_args()
    clear_schema_registry_for_import(args.schema_registry_url, args.subject)
    schema_registry = lsst.alert.packet.schemaRegistry.SchemaRegistry().all_schemas_from_filesystem()
    upload_schemas(
        args.schema_registry_url,
        subject=args.subject,
        schema_registry=schema_registry
    )
    close_schema_registry(args.schema_registry_url, args.subject)

    # Pick one version to check, e.g., the last one processed or a known one.
    # For simplicity, let's check one if we have any schemas.
    if schema_registry.known_versions:
        # Just pick an arbitrary version to check
        test_version = list(schema_registry.known_versions)[0] 
        check_metadata(args.schema_registry_url, args.subject, test_version)


if __name__ == "__main__":
    main()
