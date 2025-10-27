#!/usr/bin/env python
"""Setup specs for packaging, distributing, and installing Pipeline lib."""

import setuptools

# To debug, set DISTUTILS_DEBUG env var to anything.
setuptools.setup(
    name="GoogleAppEnginePipeline",
    version="2.1.0",
    packages=setuptools.find_packages(),
    author="Google App Engine",
    author_email="app-engine-pipeline-api@googlegroups.com",
    keywords="google app engine pipeline data processing",
    url="https://github.com/GoogleCloudPlatform/appengine-pipelines",
    license="Apache License 2.0",
    description=("Enable asynchronous pipeline style data processing on "
                 "App Engine"),
    python_requires='>=3.9',
    zip_safe=True,
    include_package_data=True,
    # Exclude these files from installation.
    exclude_package_data={"": ["README"]},
    install_requires=[
        'google-cloud-ndb>=2.3.0',
        'google-cloud-tasks>=2.16.0',
        'google-cloud-storage>=2.0.0',
        'flask>=3.0.0',
    ],
)
