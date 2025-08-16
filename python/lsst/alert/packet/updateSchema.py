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
import os
import fastavro
import yaml
import json


__all__ = ['generate_schema']


def write_schema(schema, path):

    if not os.path.exists(path):
        if not os.path.exists(path[0:-2]):
            os.mkdir(path[0:-2])
            os.mkdir(path)
        else:
            os.mkdir(path)

    updated_schema_name = schema['namespace'] + '.' + schema['name'] + '.avsc'

    with open(path + '/' + updated_schema_name, "w") as f:
        json.dump(schema, f, indent=2)


def add_namespace(schema):

    schema_names = schema['name'].split(".")
    schema['name'] = schema_names[2]
    schema['namespace'] = schema_names[0] + "." + schema_names[1]
    schema = dict(sorted(schema.items(), reverse=True))

    return schema


def populate_fields(apdb_table):
    """Make a dictionary of fields to populate the avro schema. At present, a
    number of rules must be included to ensure certain fields are excluded.

    Parameters
    ----------
    apdb_table: `dict`
        The dictionary used to generate the avro schema.
    """

    field_dictionary_array = []
    for column in apdb_table['columns']:

        # exclude fields used only for updates after PP runs
        excluded_fields = ['validityEnd', 'time_withdrawn', 'ssObjectReassocTime']
        exclude = False
        for excluded_field in excluded_fields:
            if excluded_field in column['name']:
                exclude = True

        if not exclude:
            if 'char' in column['datatype']:
                column['datatype'] = 'string'

            if 'short' in column['datatype']:
                column['datatype'] = 'int'

            if 'timestamp' in column['datatype']:
                column['datatype'] = {'type': 'long', 'logicalType': 'timestamp-micros'}
            else:
                column['datatype'] = str(column['datatype'])

            doc = ''
            if 'description' in column:
                doc = column['description']
            if 'fits:tunit' in column:
                unit = column['fits:tunit']
                if unit:
                    if doc.endswith('.'):
                        doc = doc[:-1]
                    doc += f" [{unit}]."

            # Check if a column is nullable. If it is, it needs a default.
            if 'nullable' in column:
                if column['nullable'] is False:
                    field = {'name': column['name'],
                             'type': column['datatype'],
                             'doc': doc}
                else:  # nullable == True
                    field = {'name': column['name'],
                             'type': ['null', column['datatype']],
                             'doc': doc, 'default': None}
            else:  # nullable not in columns (nullable == True)
                field = {'name': column['name'],
                         'type': ['null', column['datatype']],
                         'doc': doc, 'default': None}

            field_dictionary_array.append(field)

    return field_dictionary_array


def create_schema(name, field_dictionary_list, version):
    """ Create a schema using a field dictionary. fastavro will automatically
    take the name and namespace and put them as one, so the name should just be
    the schema name and the namespace needs to be created separately. The
    fastavro keys also need to be removed from the schema.

    Parameters
    ----------
    name: `string`
        The name of the schema as a string. (e.g., `'diaSource'`).

    field_dictionary_list: 'list'
        A list containing dictionary entries for the individual fields.

    version: 'string'
        The version number of the schema.
    """
    if name != 'MPCORB':
        name = name[0:2].lower() + name[2:]
    schema = fastavro.parse_schema({
        "name": name,
        "type": "record",
        "fields": field_dictionary_list
    })

    schema['namespace'] = 'lsst.v' + version
    fastavro_keys = list(schema.keys())
    for key in fastavro_keys:
        if '__' in key and '__len__' not in key:
            schema.pop(key)

    schema = dict(sorted(schema.items(), reverse=True))

    return schema


def generate_schema(apdb_filepath, schema_path, schema_version):
    """Generate avro schemas using an apdb.yaml file.

    Using a provided path to the apdb.yaml file and schema folder,
    generate a new schema, using a provided version number.

    Parameters
    ----------
    apdb_filepath: `string`
        Input path to the apdb.yaml file which contains the information
        used to generate the new schemas.
        Example: path/to/sdm_schemas/yml/apdb.yaml

    schema_path: `string`
        Input path to the schema folder where the new schemas will
        be added.
        Example: /path/to/alert_packet/python/lsst/alert/packet/schema

    schema_version: 'string'
        Provide the version number of the schema as a string.
        Example: "5.1"

    """

    path = os.path.join(schema_path, *schema_version.split("."))

    with open(apdb_filepath, 'r') as file:
        apdb = yaml.safe_load(file)

    version_name = schema_version.split(".")[0] + "_" + schema_version.split(".")[1]

    table_names = ['DiaForcedSource', 'DiaObject', 'DiaSource', 'SSSource', 'MPCORB']
    for name in table_names:

        for table in apdb['tables']:
            if table['name'] == name:
                field_dictionary = populate_fields(table)
                schema = create_schema(name, field_dictionary, version_name)
                write_schema(schema, path)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Generate a schema using an apdb.yaml as the source'
                                                 'of truth and include a desired alert version number'
                                                 'Example input:'
                                                 'python3 updateSchema.py '
                                                 'Path/To/Yaml/sdm_schemas/yml/apdb.yaml '
                                                 'Path/To/alert_packet/lsst/alert/packet/schema "6.0"')
    parser.add_argument('apdb_filepath')
    parser.add_argument('schema_path')
    parser.add_argument('schema_version')

    args = parser.parse_args()

    generate_schema(args.apdb_filepath, args.schema_path, args.schema_version)
