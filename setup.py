"""
CoMeT, a config and metadata tracker.

``comet`` lives on
`GitHub <https://github.com/chime-experiment/comet>`_.
"""
from os import path
from setuptools import setup, find_packages


here = path.abspath(path.dirname(__file__))

# Get the version number without loading dependencies
with open(path.join(here, "comet", "version.py")) as f:
    for line in f:
        if line.startswith("__version__"):
            exec(line)
            break
    else:
        raise RuntimeError("Cannot find version number")

# Load the requirements from requirements.txt while removing the environment marks
with open(path.join(here, "requirements.txt")) as f:
    requirements = [line.split(";")[0].rstrip() for line in f]

setup(
    name="comet",
    version=__version__,  # wtl.metrics.__version__,
    description="Config and Metadata Tracker",
    url="https://github.com/chime-experiment/dataset-broker",
    author="Rick Nitsche",
    author_email="rick@phas.ubc.ca",
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    scripts=["scripts/comet", "scripts/comet_archiver"],
)
