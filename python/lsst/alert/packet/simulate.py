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

"""Generation of simulated Avro alrt packets.
"""
import random
import string
import numpy

__all__ = ["simulate_alert"]

def randomNull():
    """Provide a random value of the Avro `null` type.
    """
    return None

def randomBoolean():
    """Provide a random value of the Avro `boolean` type.
    """
    return random.choice([True, False])

def randomInt():
    """Provide a random value of the Avro (32 bit, signed) `int` type.
    """
    return int(numpy.random.randint(-2**31, 2**31 - 1, dtype=numpy.int32))

def randomLong():
    """Provide a random value of the Avro (64 bit, signed) `long` type.
    """
    return int(numpy.random.randint(-2**63, 2**63 - 1, dtype=numpy.int64))

def randomFloat():
    """Provide a random value of the Avro (32) bit `float` type.
    """
    return float(numpy.float32(numpy.random.random()))

def randomDouble():
    """Provide a random value of the Avro `double` type.
    """
    return float(numpy.float64(numpy.random.random()))

def randomString():
    """Provide a random value of the Avro `string` type.

    A string of length between 0 and 10 is returned.
    """
    return ''.join(random.choice(string.ascii_letters)
                   for _ in range(random.randint(0, 10)))

def randomBytes(max_bytes=1000):
    """Provide a random value of the Avro `bytes` type.

    Up to `max_bytes` bytes are returned.
    """
    return numpy.random.bytes(random.randint(0, max_bytes))

randomizerFunctionsByType = {
    'null': randomNull,
    'boolean': randomBoolean,
    'int': randomInt,
    'long': randomLong,
    'float': randomFloat,
    'double': randomDouble,
    'string': randomString,
    'bytes': randomBytes
}

def simulate_alert(schema, keepNull=None, arrayCount=None):
    """Parse the schema and generate a compliant alert with random contents.

    Parameters
    ----------
    schema : `dict`
        Schema to which to conform. Should be fully resolved (i.e., not
        contain any references to special LSST types).
    keepNull : {`list` of `str`, `None`}
        Schema keys for which to output null values.
    arrayCount : {`dict`, `None`}
        Number of array items to randomly generate for each provided schema key.

    Returns
    -------
    output : `dict`
        Packet with random values corresponding to provided schema.

    Notes
    -----
    `keepNull` and `arrayCount` expect schema keys without namespaces
    (e.g., `'diaSourceId'`, not `'lsst.alert.diaSource.diaSourceId'`.
    This is sufficient because our schemas have unique keys but is
    not fully general.

    Todo
    ----
    This should accept an instance of `lsst.alert.Schema` and do whatever
    resolution is necessary internally.
    """

    output = {}

    if keepNull is None:
        keepNull = []

    if arrayCount is None:
        arrayCount = {}

    if type(schema['type']) == list:
        # potentially nullable
        if ('null' in schema['type']) and (schema['name'] in keepNull):
            return {schema['name']: None}
        else:
            # inferring the type like this this is not general but works
            # for our application
            schema['type'] = schema['type'][0]

    if type(schema['type']) == dict:
        # either an array, a record, or a nested type
        if schema['type']['type'] == 'array':
            if schema['name'] in arrayCount:
                output_array = []
                for i in range(arrayCount[schema['name']]):
                    output_array.append(simulate_alert(
                        schema['type']['items'], keepNull=keepNull, arrayCount=arrayCount))
                return {schema['name']: output_array}
            else:
                return {schema['name']: None}
        elif schema['type'] == 'record':
            for field in schema['fields']:
                output.update(simulate_alert(field, keepNull=keepNull, arrayCount=arrayCount))
            return output
        else:
            # a nested type
            output.update({schema['name']: simulate_alert(
                schema['type'], keepNull=keepNull, arrayCount=arrayCount)})
            return output

    if schema['type'] == 'record':
        for field in schema['fields']:
            output.update(simulate_alert(field, keepNull=keepNull, arrayCount=arrayCount))
        return output
    elif schema['type'] in randomizerFunctionsByType.keys():
        return {schema['name']: randomizerFunctionsByType[schema['type']]()}

    raise ValueError('Parsing broke...')
