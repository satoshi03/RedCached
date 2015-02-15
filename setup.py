# -*- coding:utf-8 -*-

try:
  import setuptools
  from setuptools import setup, find_packages
except ImportError:
  print("Please install setuptools.")

import os
import sys
import info
import version

libdir = "."

setup_options = info.INFO
setup_options["version"] = version.VERSION
setup_options.update(dict(
  install_requires = open('requirements.txt').read().splitlines(),
  packages         = find_packages(libdir),
  package_dir      = {"": libdir},
))

setup(**setup_options)
