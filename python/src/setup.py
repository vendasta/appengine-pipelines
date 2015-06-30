#!/usr/bin/env python
"""Setup specs for packaging, distributing, and installing Pipeline lib."""

import setuptools

setuptools.setup(
    name="pipeline",
    version="1.2.0",
    packages=setuptools.find_packages(),
    author="Kevin Sookocheff",
    author_email="ksookocheff@vendasta.com",
    keywords="google app engine pipeline data processing",
    url="https://github.com/vendasta/appengine-mapreduce.git",
    license="Apache License 2.0",
    description=("Enable asynchronous pipeline style data processing on "
                 "App Engine"),
    zip_safe=True,
    include_package_data=True,
    # Exclude these files from installation.
    exclude_package_data={"": ["README"]},
    install_requires=[
        "GoogleAppEngineCloudStorageClient >= 1.9.21",
    ]
)
