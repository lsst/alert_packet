"""Generate Avro packets with randomized values given input schemas."""

import string
import random
from io import BytesIO
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
    return float(((np.random.rand() - 0.5) * 2. * np.finfo(np.float32).max).astype(np.float32))


def randomDouble():
    """Provide a random value of the Avro `double` type.
    """
    return float(((np.random.rand() - 0.5) * 2. * np.finfo(np.float64).max).astype(np.float64))


def randomString():
    """Provide a random value of the Avro `string` type.
    """
    def rand_str(n): return ''.join([random.choice(string.ascii_lowercase) for i in range(n)])
    return rand_str(np.random.randint(0, 10))

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

    Parameters
    ----------
    keepNull : {`list` of `str`, `None`}
        schema keys to output null values for.
    arrayCount : {`dict`, `None`}
        number of array items to randomly generate for each provided schema key

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
            output.update({schema['name']: createFakeJsonPacket(
                schema['type'], keepNull=keepNull, arrayCount=arrayCount)})
            return output

    if schema['type'] == 'record':
        for field in schema['fields']:
            output.update(createFakeJsonPacket(field, keepNull=keepNull, arrayCount=arrayCount))
        return output
    elif schema['type'] in randomizerFunctionsByType.keys():
        return {schema['name']: randomizerFunctionsByType[schema['type']]()}

    raise ValueError('Parsing broke...')



def loadSchemas():
    """Combine standard LSST schemas into JSON.
    """

    schemas = ['schema/cutout.avsc', 'schema/diaforcedsource.avsc',
               'schema/dianondetectionlimit.avsc', 'schema/diasource.avsc',
               'schema/diaobject.avsc', 'schema/ssobject.avsc', 'schema/alert.avsc']

    return combine_schemas(schemas)


def createFakePacket(jsonSchema, writeSchema=False, outfile='data/fakeAlert.avro', arrayCount=None, keepNull=None):
    """Serialize a fake packet to disk.


    Parameters
    ----------
    jsonSchema : `dict`
        Schema of the packet to read
    writeSchema : `bool`
       Serialize schema in the alert packet? (default False)
    arrayCount : `dict` or `None`
        Schema keys of array fields and number of array elements to generate.
    arrayCount : {`dict`, `None`}
        number of array items to randomly generate for each provided schema key
        Default `None` conservatively assumes a year of previous DIASources, 
        a month of forced sources, and no previous limits.
    keepNull : {`list` of `str`, `None`}
        Schema keys to output null values for.
        Default `None` sets `ssObject` to null.
    """

    # values from LSE-81
    avgYearlyVisits = int(1056/10)

    if keepNull is None:
        keepNull = ['ssObject']
    if arrayCount is None:
        arrayCount = {'prvDiaSources': avgYearlyVisits,
                      'prvDiaForcedSources': int(avgYearlyVisits/12),
                      'prvDiaNondetectionLimits': 0}

    jsonData = createFakeJsonPacket(jsonSchema, keepNull=keepNull, arrayCount=arrayCount)

    if not fastavro.validate(jsonData, jsonSchema):
        raise ValueError("Error: generated packet does not validate")

    with open(outfile, 'wb') as f:
        if writeSchema:
            fastavro.writer(f, jsonSchema, [jsonData])
        else:
            fastavro.schemaless_writer(f, jsonSchema, jsonData)

    return jsonData


def readFakePacket(jsonSchema, writeSchema=False, outfile='data/fakeAlert.avro'):
    """Read a fake packet serialized to disk.

    Parameters
    ----------
    jsonSchema : `dict`
        Schema of the packet to read
    writeSchema : `bool`
        Was the schema included in the alert packet? (default False)
    """

    with open(outfile, 'rb') as f:
        if writeSchema:
            packets = [p for p in fastavro.reader(f)]
            packet = packets[0]
        else:
            packet = fastavro.schemaless_reader(f, jsonSchema)

    return packet


def main():
    """Wrap fake generation code.
    """

    jsonSchema = loadSchemas()
    fakeData = createFakePacket(jsonSchema)
    readData = readFakePacket(jsonSchema)

    assert(fakeData == readData)
    assert(fastavro.validate(readData, jsonSchema))

if __name__ == '__main__':
    main()
