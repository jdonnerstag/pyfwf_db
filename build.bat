@echo on

REM Package the project and create a "Source Distribution" and a Pyhton
REM wheel (binary distribution)
REM TODO Still required with pyproject.toml
python setup.py sdist bdist_wheel

REM Make the package in the current virtual environment, and make it editable
REM pip install -e .

REM For the dev tools
REM pip install -e .[dev]
