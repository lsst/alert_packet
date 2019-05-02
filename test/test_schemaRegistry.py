import json
import os
import unittest
from tempfile import TemporaryDirectory

from lsst.alert.packet import SchemaRegistry

def write_schema(root_dir, filename, version_major, version_minor):
    """Write a simple schema to a filesystem location based on its version.
    """
    # Generate a new schema for each version to avoid ID collisions.
    schema = {
        "name": "example",
        "namespace": "lsst",
        "type": "record",
        "fields": [
            {"name": "field%s%s" % (version_major, version_minor),
             "type": "int"}
        ]
    }
    target_dir = os.path.join(root_dir, version_major, version_minor)
    os.makedirs(target_dir)
    with open(os.path.join(target_dir, filename), "w") as f:
        json.dump(schema, f)

def create_filesystem_hierarchy(root_dir, root_file="lsst.example.avsc"):
    """Create a simple schema hierarchy on the filesystem.
    """
    write_schema(root_dir, root_file, "1", "0")
    write_schema(root_dir, root_file, "2", "0")
    write_schema(root_dir, root_file, "2", "1")

class FromFilesystemTestCase(unittest.TestCase):
    """Demonstrate that the SchemaRegistry can work with on-disk data.
    """

    def test_from_filesystem(self):
        """Check priming a registry based on a simple filesystem hierarchy.
        """
        with TemporaryDirectory() as tempdir:
            create_filesystem_hierarchy(tempdir)
            registry = SchemaRegistry.from_filesystem(tempdir,
                                                      schema_root="lsst.example")
            self.assertEqual(len(registry._id_to_schema), 3)
            registry.get_by_version("1.0")
            registry.get_by_version("2.0")
            registry.get_by_version("2.1")
            self.assertRaises(KeyError, registry.get_by_version, 2.2)
