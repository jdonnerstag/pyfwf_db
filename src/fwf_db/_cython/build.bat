@echo on

REM change to the directory, that contains this script
pushd %~dp0

python setup.py build_ext --inplace
popd
