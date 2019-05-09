#!/usr/bin/env python

"""
Setup script for pipe-vms-chile
"""

import codecs
import os

from setuptools import find_packages
from setuptools import setup

# [TODO]: Edit the package name here.  Note the '-' vs '_'
PACKAGE_NAME='pipe-vms-chile'
package = __import__('pipe_vms_chile')


DEPENDENCIES = [
    "nose",
    "ujson",
    "pytz",
    "udatetime",
    "pipe-tools==0.2.4",
    "jinja2-cli",
    "requests==2.9.2",
    "newlinejson==1.0",
    "argparse==1.2.1",
    "psycopg2-binary"
]

with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()

with codecs.open('requirements.txt', encoding='utf-8') as f:
    DEPENDENCY_LINKS=[line for line in f]

setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    install_requires=DEPENDENCIES,
    license="Apache 2.0",
    long_description=readme,
    name=PACKAGE_NAME,
    url=package.__source__,
    version=package.__version__,
    zip_safe=True,
    dependency_links=DEPENDENCY_LINKS
)