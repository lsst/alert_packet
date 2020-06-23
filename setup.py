from setuptools import setup

# version.py is created when building with SCons.
# Otherwise, we create it ourselves.
version = "0.1.0"
with open("./python/lsst/alert/packet/version.py", "w") as f:
    print(f"""
__all__ = ("__version__", )
__version__='{version}'""", file=f)

# All configuration is in setup.cfg.
setup(version=f"{version}")
