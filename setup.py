"""Package configuration."""
from setuptools import find_packages, setup

setup(
    name="2_de_assignment_spark_job",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)
