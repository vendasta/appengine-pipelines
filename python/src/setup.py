#!/usr/bin/env python
"""Setup specs for packaging, distributing, and installing Pipeline lib."""

import setuptools

# To debug, set DISTUTILS_DEBUG env var to anything.
setuptools.setup(
    name="GoogleAppEnginePipeline",
    version="2.0.0a13",
    packages=setuptools.find_packages(),
    author="Google App Engine",
    author_email="app-engine-pipeline-api@googlegroups.com",
    keywords="google app engine pipeline data processing",
    url="https://github.com/GoogleCloudPlatform/appengine-pipelines",
    license="Apache License 2.0",
    description=("Enable asynchronous pipeline style data processing on "
                 "App Engine"),
    zip_safe=True,
    include_package_data=True,
    # Exclude these files from installation.
    exclude_package_data={"": ["README"]},
    install_requires=[],
)
