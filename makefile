.PHONY: build dist-test dist
SHELL := /bin/bash

build:
	python3 -m pip install --upgrade build
	python3 -m build

dist-test:
	python3 -m pip install --user --upgrade twine
	python3 -m twine upload --repository testpypi dist/*

dist:
	python3 -m pip install --user --upgrade twine
	python3 -m twine upload dist/*
