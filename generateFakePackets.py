

import numpy as np
from validateAvroNestedSchema import load_stamp, combine_schemas

cutout_path = './examples/stamp-676.fits.gz'

schemas = ['schema/cutout.avsc','schema/diaforcedsource.avsc',
           'schema/dianondetectionlimit.avsc','schema/diasource.avsc',
           'schema/diaobject.avsc','schema/ssobject.avsc','schema/alert.avsc']

schema = combine_schemas(schemas)


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
    return np.random.randint(np.iinfo(np.int32).min, 
                             high = np.iinfo(np.int32).max, dtype=np.int32)

def randomLong():
    """Provide a random value of the Avro `long` type.
    """
    return np.random.randint(np.iinfo(np.int64).min, 
                             high = np.iinfo(np.int64).max, dtype=np.int64)

def randomFloat():
    """Provide a random value of the Avro `float` type.
    """
    return (np.random.rand() * np.finfo(np.float32).max).astype(np.float32)

def randomDouble():
    """Provide a random value of the Avro `double` type.
    """
    return (np.random.rand() * np.finfo(np.float64).max).astype(np.float64)

def randomString():
    """Provide a random value of the Avro `string` type.
    """
    return np.random.bytes(np.random.randint(0,100)).decode(errors='replace')

def randomRecord():
    """Provide a random value of the Avro `record` type.
    """
    return build_random_record

def randomArray():
    """Provide a random value of the Avro `array` type.
    """
    return build_random_array

randomizerFunctionsByType = {
        'null': randomNull,
        'boolean': randomBoolean,
        'int': randomInt,
        'long': randomLong,
        'float': randomFloat,
        'double': randomDouble,
        'string': randomString
        # exclude "bytes" dtype because we will handle cutouts specially
}



def walk_schema(schema, keepNull = None, arrayCount = None):

    output = {}

    if keepNull is None:
        keepNull = []

    if arrayCount is None:
        arrayCount = {}

    if type(schema['type']) == list:
        # potentially nullable
        if ('null' in schema['type']) and (schema['name'] in keepNull):
            print(schema['name'], schema['type'])
            return {schema['name']: None}
        else:
            # this is not general, but our schema is only fixed types and nulls
            schema['type'] = schema['type'][0]

    if type(schema['type']) == dict:
        # either an array, a record, or a nested type
        if schema['type']['type'] == 'array':
            if schema['name'] in arrayCount:
                output_array = []
                for i in range(arrayCount[schema['name']]):
                    output_array.append(walk_schema(schema['type']['items']))
                return {schema['name']: output_array}
            else:
                return {schema['name']: None}
        elif schema['type'] == 'record':
            for field in schema['fields']:
                output.update(walk_schema(field))
                return output
        else:
            # a nested type 
            nest = {schema['name']:walk_schema(schema['type'])}
            output.update(nest)
            return output

    if schema['type'] == 'record':
        for field in schema['fields']:
            output.update(walk_schema(field))
    elif schema['type'] in ['null', 'boolean', 'int', 'long', 'float', 'double', 'string']:
        return {schema['name'] : randomizerFunctionsByType[schema['type']]()}
    elif schema['type'] == 'bytes':
        return {schema['name'] : load_stamp(cutout_path) }
    else:
        raise ValueError('Parsing broke...')

    return output
