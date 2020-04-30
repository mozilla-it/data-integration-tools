import os
import setuptools
from setuptools import setup, find_packages

setup(
    name="integration-tools",
    version="0.0.1",
    description="Common python tools for accessing data integrations in mozilla-it",
    python_requires=">=3.4",
    author="Mozilla IT Integration Platform",
    author_email="afrank@mozilla.com",
    packages=find_packages(),
    install_requires=["google-cloud-bigquery"],
    project_urls={"Source": "https://github.com/mozilla-it/integration-tools"},
)

