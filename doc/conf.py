"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.alert.packet


_g = globals()
_g.update(build_package_configs(
    project_name='alert_packet',
    version=lsst.alert.packet.version.__version__))
