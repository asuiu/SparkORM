#!/bin/bash
rm ./dist/*.whl
poetry build
twine upload dist/*.whl