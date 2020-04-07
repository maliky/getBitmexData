# -*- coding: utf-8 -*-
# https://realpython.com/pypi-publish-python-package/
import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.org").read_text()

# This call to setup() does all the work
setup(
    name="getBitmexData",  # ou maliky
    version="1.0.0",
    description="Download Bitmex historical data with different time resolution.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/maliky/getBitmexData",
    author="Malik Koné",
    author_email="malikykone@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    pacakges=find_packages(exclude=("Tests",)),  # ou packages=["getBitmexData"],
    include_package_data=True,
    install_requires=["pandas", "request"],
    entry_points={"console_scripts": ["getBitmexData=getBitmexData.__main__:main",]},
)
