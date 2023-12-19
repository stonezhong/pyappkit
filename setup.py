import os
from setuptools import setup, find_packages

# The directory containing this file
HERE = os.path.dirname(os.path.abspath(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md"), "r") as f:
    README = f.read()

# This call to setup() does all the work
setup(
    name="pyappkit",
    version="0.0.14",
    description="Python Application Kit",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/stonezhong/pyappkit",
    author="Stone Zhong",
    author_email="stonezhong@hotmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=["jinja2", "PyYAML"],
    entry_points={
        "console_scripts": [
            "daemon=pyappkit.daemon.main:daemon",
        ]
    },
)
