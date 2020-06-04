from setuptools import setup

import os

package_files = []
for dir, subdirs, files in os.walk("schema"):
    for d in subdirs:
        package_files.append(dir + "/" + d)
    for f in files:
        package_files.append(dir + "/" + f)

setup(name='lsst-alert-packet',
      version="0.1.0",
      description='Code for interacting with Vera C. Rubin Observatory alert packets',
      url='https://github.com/lsst/alert_packet',
      classifiers=[
          'Programming Language :: Python :: 3',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Development Status :: 3 - Alpha',
      ],
      author='Spencer Nelson',
      author_email='swnelson@uw.edu',
      license='GPLv3',
      packages=['lsst.alert.packet'],
      package_dir={'': 'python'},
      package_data={'lsst.alert.packet': package_files},
      install_requires=['fastavro', 'numpy'],
      scripts=[
          'bin/simulateAlerts.py',
          'bin/validateAvroRoundTrip.py',
      ])
