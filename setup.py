#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/29 16:07
@File    : setup.py
@contact : mmmaaaggg@163.com
@desc    : 
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding='utf-8') as rm:
    long_description = rm.read()

setup(name='DIRestInvoker',
      version='0.2.2',
      description='调用 DIRestPlus 接口，实现Wind、iFinD、Choice接口调用',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='MG',
      author_email='mmmaaaggg@163.com',
      url='https://github.com/DataIntegrationAlliance/DIRestInvoker',
      packages=find_packages(),
      python_requires='>=3.5',
      classifiers=(
          "Programming Language :: Python :: 3 :: Only",
          "Programming Language :: Python :: 3.5",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "License :: OSI Approved :: MIT License",
          "Operating System :: MacOS",
          "Operating System :: Microsoft :: Windows",
          "Operating System :: Unix",
          "Operating System :: POSIX",
          "Operating System :: OS Independent",
          "Development Status :: 5 - Production/Stable",
          "Environment :: No Input/Output (Daemon)",
          "Intended Audience :: Developers",
          "Natural Language :: Chinese (Simplified)",
          "Topic :: Software Development",
      ),
      install_requires=[
          'pandas>=0.23.0',
          'requests>=2.19.1',
          'xlrd>=1.1.0',
      ])
