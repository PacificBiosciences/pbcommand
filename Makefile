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
	rm -rf docs/build
	rm FileTypes.h

test-nose:
	nosetests -s --verbose --with-xunit --logging-config log_nose.cfg tests/test_*.py

test: test-nose run-pylint run-pep8

doc:
	cd docs && make html

build-tool-contracts:
	python -m pbcommand.cli.examples.dev_app --emit-tool-contract > ./tests/data/tool-contracts/pbcommand.tasks.dev_app_tool_contract.json
	python -m pbcommand.cli.examples.dev_app --emit-tool-contract > ./tests/data/tool-contracts/dev_example_tool_contract.json
	python -m pbcommand.cli.examples.dev_txt_app --emit-tool-contract > ./tests/data/tool-contracts/dev_example_dev_txt_app_tool_contract.json
	python -m pbcommand.cli.examples.dev_mixed_app --emit-tool-contract > ./tests/data/tool-contracts/dev_mixed_app_tool_contract.json
	python -m pbcommand.cli.examples.dev_gather_fasta_app --emit-tool-contract > ./tests/data/tool-contracts/dev_gather_fasta_app_tool_contract.json
	python -m pbcommand.cli.examples.dev_scatter_fasta_app --emit-tool-contract > ./tests/data/tool-contracts/dev_scatter_fasta_app_tool_contract.json
	python -m pbcommand.cli.examples.dev_quick_hello_world emit-tool-contracts -o ./tests/data/tool-contracts

run-pylint:
	pylint --errors-only pbcommand

run-pep8:
	# use xargs to propagate exit code
	find pbcommand -name "*.py" | xargs pep8 --ignore=E501,E265,E731,E402,W292

run-auto-pep8:
	find pbcommand -name "*.py" -exec autopep8 -i --ignore=E501,E265,E731,E402,W292 {} \;

build-java-classes:
	avro-tools compile schema pbcommand/schemas java-classes/

extract-readme-snippets:
	rm -rf readme-snippet-*.py
	pandoc -t markdown README.md  | pandoc --filter ./extract-readme-snippets.py

build-avro-schema-docs:
	# this requires nodejs + https://github.com/ept/avrodoc
	avrodoc pbcommand/schemas/*.avsc > index.html

cpp-header:
	python extras/make_cpp_file_types_header.py FileTypes.h
