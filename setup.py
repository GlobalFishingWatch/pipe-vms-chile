#!/usr/bin/env python

"""
Setup script for pipe-vms-chile
"""
from setuptools import setup

import codecs

PACKAGE_NAME='pipe-vms-chile'

package = __import__('pipe_vms_chile')


with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()

setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    install_requires=[],
    license="Apache 2.0",
    long_description=readme,
    name=PACKAGE_NAME,
    url=package.__source__,
    version=package.__version__,
    zip_safe=True
)
