#!/usr/bin/python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import setuptools

release_status = 'Development Status :: 1 - Beta'

with open('README.md') as readme_file:
    readme = readme_file.read()

setuptools.setup(
    name='google-datacatalog-mysql-lineage-extractor',
    version='0.1.0',
    author='Google LLC',
    description='Library for extracting lineage from MySQL queries',
    platforms='Posix; MacOS X; Windows',
    packages=setuptools.find_packages(where='./src') + ['jars'],
    package_dir={
        '': 'src'
    },
    include_package_data=True,
    install_requires=('iteration-utilities', 'jpype1'),
    setup_requires=('pytest-runner'),
    tests_require=('pytest-cov'),
    classifiers=(
        release_status,
        'Programming Language :: Python :: 3.7',
    ),
    long_description=readme,
    long_description_content_type='text/markdown',
)