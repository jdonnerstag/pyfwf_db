@echo on

REM Package the project and create a "Source Distribution" and a Pyhton
REM wheel (binary distribution)
python setup.py sdist bdist_wheel

REM Make the package in the current virtual environment, and make it editable
REM pip install -e .
