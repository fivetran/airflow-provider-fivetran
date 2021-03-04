.PHONY: build dist
SHELL := /bin/bash

build:
	python3 -m pip install --upgrade build
	python3 -m build

dist:
	python3 -m pip install --user --upgrade twine
	python3 -m twine upload dist/*
