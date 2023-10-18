#!/usr/bin/env python

from setuptools import find_packages
from setuptools import setup

setup(
    version='v4.1.1',
    author='engineering@globalfishingwatch.org',
    packages=find_packages(exclude=['test*.*', 'tests'])
)

