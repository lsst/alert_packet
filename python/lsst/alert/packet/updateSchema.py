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
from lsst.alert.packet import get_schema_root, SchemaRegistry


__all__ = ['update_schema']


def write_schema(schema, version, path):

    updated_schema_loc = path[0:-3] + version[0]+'/'+version[2]

    if not os.path.exists(updated_schema_loc):
        if not os.path.exists(updated_schema_loc[0:-2]):
            os.mkdir(updated_schema_loc[0:-2])
            os.mkdir(updated_schema_loc)
        else:
            os.mkdir(updated_schema_loc)

    updated_schema_name = schema['namespace'] + '.' + schema['name'] + '.avsc'

    with open(updated_schema_loc + '/' + updated_schema_name, "w") as f:
        json.dump(schema, f, indent=2)


def add_namespace(schema):

    schema_names = schema['name'].split(".")
    schema['name'] = schema_names[2]
    schema['namespace'] = schema_names[0] + "." + schema_names[1]
    schema = dict(sorted(schema.items(), reverse=True))

    return schema


def populate_fields(apdb):
    """Make a dictionary of fields to populate the avro schema. At present, a
    number of rules must be included to ensure certain fields are excluded.

    Parameters
    ----------
    apdb: `dict`
        The name of the schema as a string. E.G. diaSource.
    """

    field_dictionary_array = []
    for column in apdb['columns']:
        # We are still finalizing the time series feature names.
        if (column['name'] != 'validityStart') and (
                column['name'] != 'validityEnd') and "Periodic" not in column[
                'name'] and "max" not in column['name'] and "min" not in column[
                'name'] and "Science" not in column['name'] and "Percentile" not in column[
                'name'] and "Max" not in column['name'] and "Min" not in column[
                'name'] and "science" not in column['name'] and "LowzGal" not in column[
                'name'] and "MAD" not in column['name'] and "Skew" not in column[
                'name'] and "Intercept" not in column['name'] and "Slope" not in column[
                'name'] and "Stetson" not in column['name'] and "lastNonForcedSource" not in column[
                'name'] and "nDiaSources" not in column['name'] and "ExtObj" not in column[
                'name'] and "time_" not in column['name'] and "Time" not in column[
                'name'] and "isDipole" not in column['name'] and "bboxSize" not in column['name']:

            if 'char' in column['datatype']:
                column['datatype'] = "string"
            # Check if a column is nullable. If it is, it needs a default.
            if 'nullable' in column:
                if column['nullable'] is False:
                    # Check if a column has a description, if so, include "doc"
                    if 'description' in column:
                        field = {"name": column['name'],
                                 "type": column["datatype"],
                                 "doc": column["description"]}
                        field_dictionary_array.append(field)
                    else:
                        field = {"name": column['name'],
                                 "type": column["datatype"], "doc": ""}
                        field_dictionary_array.append(field)
                else:  # nullable == True
                    if 'description' in column:
                        field = {"name": column['name'],
                                 "type": ["null", str(column["datatype"])],
                                 "doc": column["description"], "default": None}
                        field_dictionary_array.append(field)
                    else:
                        field = {"name": column['name'],
                                 "type": ["null", str(column["datatype"])],
                                 "doc": "", "default": None}
                        field_dictionary_array.append(field)
            else:  # nullable not in columns (nullable == True)
                if 'description' in column:
                    field = {"name": column['name'],
                             "type": ["null", str(column["datatype"])],
                             "doc": column["description"], "default": None}
                    field_dictionary_array.append(field)
                else:
                    field = {"name": column['name'],
                             "type": ["null", str(column["datatype"])],
                             "doc": "", "default": None}
                    field_dictionary_array.append(field)

    return field_dictionary_array


def create_schema(name, field_dictionary_array, version):
    """ Create a schema using the field dictionary. fastavro will automatically
    take the name and namespace and put them as one, so the name should just be
    the schema name and the namespace needs to be created separately. The
    fastavro keys also need to be removed from the schema.

        Parameters
    ----------
    name: `string`
        The name of the schema as a string. E.G. diaSource.

    field_dictionary_array: 'np.array'
        An array containing dictionary entries for the individual fields.

    version: 'string'
        The version number of the schema
    """
    name = name[0:2].lower() + name[2:]
    schema = fastavro.parse_schema({
        "name": name,
        "type": "record",
        "fields": field_dictionary_array
    })

    schema['namespace'] = 'lsst.v' + version
    fastavro_keys = list(schema.keys())
    for key in fastavro_keys:
        if '__' in key and '__len__' not in key:
            schema.pop(key)

    schema = dict(sorted(schema.items(), reverse=True))

    return schema


def update_schema(apdb_filepath, update_version=None):
    """Compare an avro schemas  docstrings with the apdb.yaml file.

    If there are no docstrings, add the field. If the docstrings do
    not match, update the schema docstrings to the apdb docstrings.

    Once it is updated, write out the new schema.

    Parameters
    ----------
    apdb_filepath: `string`
        Input string for the apdb.yaml file where the docstrings
        will be compared.

    update_version: 'string'
        If a string is included, update schema to provided version.
        Example: "5.1"

    """

    registry = SchemaRegistry.from_filesystem()

    for version in registry.known_versions:
        schema_root = get_schema_root()
        path = os.path.join(schema_root, *version.split("."))

        with open(apdb_filepath, 'r') as file:
            apdb = yaml.safe_load(file)

        if update_version:
            version_name = update_version.split(".")[0] + "_" + update_version.split(".")[1]
        else:
            version_name = version.split(".")[0] + "_" + version.split(".")[1]

        # The first 4 columns in the apdb are the ones we use for alerts
        for x in range(0, 4):

            name = apdb['tables'][x]['name']
            field_dictionary = populate_fields(apdb['tables'][x])
            schema = create_schema(name, field_dictionary, version_name)

            write_schema(schema, version_name, path)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Update the schema docstrings so that they'
                                                 'match the docstrings in apdb.yaml and include the '
                                                 'desired version number. Example input:'
                                                 'python3 updateSchema.py '
                                                 '"Path/To/Yaml/sdm_schemas/yml/apdb.yaml" "6.0"')
    parser.add_argument('apdb_filepath')
    parser.add_argument('update_version')

    args = parser.parse_args()

    update_schema(args.apdb_filepath, args.update_version)
