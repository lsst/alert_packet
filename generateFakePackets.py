

import string
import random
import numpy as np
import fastavro
from validateAvroNestedSchema import load_stamp, combine_schemas


def randomNull():
    """Provide a random value of the Avro `null` type.
    """
    return None


def randomBoolean():
    """Provide a random value of the Avro `boolean` type.
    """
    return bool(np.random.randint(2))


def randomInt():
    """Provide a random value of the Avro `int` type.
    """
    return int(np.random.randint(np.iinfo(np.int32).min,
                                 high=np.iinfo(np.int32).max, dtype=np.int32))


def randomLong():
    """Provide a random value of the Avro `long` type.
    """
    return int(np.random.randint(np.iinfo(np.int64).min,
                                 high=np.iinfo(np.int64).max, dtype=np.int64))


def randomFloat():
    """Provide a random value of the Avro `float` type.
    """
    return float((np.random.rand() * np.finfo(np.float32).max).astype(np.float32))


def randomDouble():
    """Provide a random value of the Avro `double` type.
    """
    return float((np.random.rand() * np.finfo(np.float64).max).astype(np.float64))


def randomString():
    """Provide a random value of the Avro `string` type.
    """
    def rand_str(n): return ''.join([random.choice(string.ascii_lowercase) for i in range(n)])
    return rand_str(np.random.randint(0, 100))


def randomRecord():
    """Provide a random value of the Avro `record` type.
    """
    return build_random_record


def randomArray():
    """Provide a random value of the Avro `array` type.
    """
    return build_random_array


def randomBytes():
    """Provide a random value of the Avro `bytes` type.
    """
    cutout_path = './examples/stamp-676.fits.gz'
    return load_stamp(cutout_path)['stampData']


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


def createFakeJsonPacket(schema, keepNull=None, arrayCount=None):
    """Parse JSON schema and generate random values.
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
                    output_array.append(createFakeJsonPacket(
                        schema['type']['items'], keepNull=keepNull, arrayCount=arrayCount))
                return {schema['name']: output_array}
            else:
                return {schema['name']: None}
        elif schema['type'] == 'record':
            for field in schema['fields']:
                output.update(createFakeJsonPacket(field, keepNull=keepNull, arrayCount=arrayCount))
                return output
        else:
            # a nested type
            nest = {schema['name']: createFakeJsonPacket(
                schema['type'], keepNull=keepNull, arrayCount=arrayCount)}
            output.update(nest)
            return output

    if schema['type'] == 'record':
        for field in schema['fields']:
            output.update(createFakeJsonPacket(field, keepNull=keepNull, arrayCount=arrayCount))
    elif schema['type'] in randomizerFunctionsByType.keys():
        return {schema['name']: randomizerFunctionsByType[schema['type']]()}
    else:
        raise ValueError('Parsing broke...')

    return output


def createFakePacket(writeSchema=False):
    """Serialize a fake packet to disk.
    """

    outfile = 'test.avro'

    schemas = ['schema/cutout.avsc', 'schema/diaforcedsource.avsc',
               'schema/dianondetectionlimit.avsc', 'schema/diasource.avsc',
               'schema/diaobject.avsc', 'schema/ssobject.avsc', 'schema/alert.avsc']

    jsonSchema = combine_schemas(schemas)

    # values from LSE-81
    avgYearlyVisits = int(1056/10)

    keepNull = ['ssObject']
    arrayCount = {'prvDiaSources': avgYearlyVisits,
                  'prvDiaForcedSources': int(avgYearlyVisits/12),
                  'prvDiaNondetectionLimits': 0}

    jsonData = createFakeJsonPacket(jsonSchema, keepNull=keepNull, arrayCount=arrayCount)

    with open(outfile, 'wb') as f:
        if writeSchema:
            fastavro.writer(f, jsonSchema, jsonData)
        else:
            fastavro.schemaless_writer(f, jsonSchema, jsonData)
