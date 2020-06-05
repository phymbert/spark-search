#!/usr/bin/env python

#
# Copyright 2020 the Spark Search contributors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import print_function

import glob
import os
import sys
from shutil import copytree, rmtree

from setuptools import setup

try:
    exec(open('pysparksearch/version.py').read())
except IOError:
    print("Failed to load PySparkSearch version file for packaging. You must be in Spark Search's python dir.",
          file=sys.stderr)
    sys.exit(-1)
VERSION = __version__  # noqa
# A temporary path so we can access above the Python project root and fetch scripts and jars we need
TEMP_PATH = "deps"
SPARK_SEARCH_HOME = os.path.abspath("../")

# Provide guidance about how to use setup.py
incorrect_invocation_message = """
If you are installing pysparksearch from spark search source, you must first build Spark and
run sdist.

    To build Spark Search with maven you can run:
      ./build/mvn -DskipTests clean package
    Building the source dist is done in the Python directory:
      cd python
      python setup.py sdist
      pip install dist/*.tar.gz"""

# Figure out where the jars are we need to package with PySparkSearch.
JARS_PATH = glob.glob(os.path.join(SPARK_SEARCH_HOME, "target/"))

if len(JARS_PATH) == 1:
    JARS_PATH = JARS_PATH[0]
elif len(JARS_PATH) == 0 and not os.path.exists(TEMP_PATH):
    print(incorrect_invocation_message, file=sys.stderr)
    sys.exit(-1)

JARS_TARGET = os.path.join(TEMP_PATH, "jars")

# Check and see if we are under the spark path in which case we need to build the symlink farm.
# This is important because we only want to build the symlink farm while under Spark otherwise we
# want to use the symlink farm. And if the symlink farm exists under while under Spark (e.g. a
# partially built sdist) we should error and have the user sort it out.
in_spark = os.path.isfile("../src/main/scala/org/apache/spark/search/package.scala")

if in_spark:
    # Construct links for setup
    try:
        os.mkdir(TEMP_PATH)
    except:
        print("Temp path for symlink to parent already exists {0}".format(TEMP_PATH),
              file=sys.stderr)
        sys.exit(-1)

try:

    if in_spark:
        copytree(JARS_PATH, JARS_TARGET)
    else:
        # If we are not inside of SPARK_SEARCH_HOME verify we have the required symlink farm
        if not os.path.exists(JARS_TARGET):
            print("To build packaging must be in the python directory under the SPARK_SEARCH_HOME.",
                  file=sys.stderr)

    # Parse the README markdown file into rst for PyPI
    long_description = "!!!!! missing pandoc do not upload to PyPI !!!!"
    try:
        import pypandoc

        long_description = pypandoc.convert_file('README.md', 'rst')
    except ImportError:
        print("Could not import pypandoc - required to package PySparkSearch", file=sys.stderr)
    except OSError:
        print("Could not convert - pandoc is not installed", file=sys.stderr)

    setup(
        name='pysparksearch',
        version=VERSION,
        description='Spark Search Python API',
        long_description=long_description,
        author='Spark Search Developers',
        author_email='pierrick.hymbert@gmail.com',
        url='https://github.com/phymbert/spark-search/tree/master/python',
        packages=['pysparksearch'],
        include_package_data=True,
        package_dir={
            'pysparksearch.jars': 'deps/jars',
        },
        package_data={
            'pysparksearch.jars': ['*.jar']
        },
        license='http://www.apache.org/licenses/LICENSE-2.0',
        install_requires=['pyspark==2.4.5'],
        setup_requires=['pypandoc'],
        classifiers=[
            'Development Status :: 0.2.0 - Development/UnStable',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy']
    )
finally:
    # We only cleanup the symlink farm if we were in Spark, otherwise we are installing rather than
    # packaging.
    if in_spark:
        rmtree(os.path.join(TEMP_PATH, "jars"))
        os.rmdir(TEMP_PATH)
