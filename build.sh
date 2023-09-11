#!/bin/bash

export PREFIX=$PREFIX
export PYTHON=$PREFIX/bin/python
export PATH=$PATH:~/.local/bin

echo "PREFIX=${PREFIX}"
echo "PYTHON=${PYTHON}"

poetry export --without-hashes --format=requirements.txt > ./requirements-poetry.txt

# Use pip to install the dependencies to a specific directory
pip install -r requirements-poetry.txt --target $PREFIX/lib/python3.10/site-packages --upgrade