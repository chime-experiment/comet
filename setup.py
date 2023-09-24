"""
CoMeT, a config and metadata tracker.

``comet`` lives on
`GitHub <https://github.com/chime-experiment/comet>`_.
"""
from os import path
from setuptools import setup, find_packages

import versioneer


here = path.abspath(path.dirname(__file__))

# Load the requirements from requirements.txt while removing the environment marks
with open(path.join(here, "requirements.txt")) as f:
    requirements = [line.split(";")[0].rstrip() for line in f]

setup(
    name="comet",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Config and Metadata Tracker",
    url="https://github.com/chime-experiment/dataset-broker",
    author="Rick Nitsche",
    author_email="rick@phas.ubc.ca",
    packages=find_packages(),
    install_requires=requirements,
    python_requires=">= 3.8",
    include_package_data=True,
    scripts=["scripts/comet", "scripts/comet_archiver"],
)
