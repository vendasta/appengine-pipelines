#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="pipeline",
    version="1.0.0",
    packages=find_packages(),
    author="VendAsta",
    author_email="jcollins@vendasta.com",
    keywords="google app engine pipelines data processing",
    url="https://github.com/vendasta/appengine-mapreduce.git",
    license="Apache License 2.0",
    description="Connects together complex workflows.",
    include_package_data=True,
    install_requires=[
            "simplejson == 2.1.1",
        ]
)
