.PHONY: all clean install dev-install test doc
SHELL = /bin/bash -e

all: install

install:
	@which pip > /dev/null
	@pip freeze|grep 'pbcommand=='>/dev/null \
      && pip uninstall -y pbcommand \
      || echo -n ''
	@pip install ./

clean:
	rm -rf build/;\
	find . -name "*.egg-info" | xargs rm -rf;\
	find . -name "*.pyc" | xargs rm -f;\
	find . -name "*.err" | xargs rm -f;\
	find . -name "*.log" | xargs rm -f;\
	rm -rf dist;\
	rm -rf docs/_build

test:
	nosetests -s --verbose --logging-config log_nose.cfg tests/test_*.py

doc:
	cd docs && make html

build-tool-contracts:
	python -m pbcommand.cli.examples.dev_txt_app --emit-tool-contract > ./tests/data/dev_example_tool_contract.json
	python -m pbcommand.cli.examples.dev_app --emit-tool-contract > ./tests/data/dev_example_dev_txt_app_contract.json
