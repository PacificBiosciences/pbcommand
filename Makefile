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
	tox

doc:
	cd docs && make html

build-tool-contracts:
	python -m pbcommand.cli.examples.dev_app --emit-tool-contract > ./tests/data/tool-contracts/pbcommand.tasks.dev_app_tool_contract.json
	python -m pbcommand.cli.examples.dev_txt_app --emit-tool-contract > ./tests/data/tool-contracts/dev_example_dev_txt_app_tool_contract.json
	python -m pbcommand.cli.examples.dev_gather_fasta_app --emit-tool-contract > ./tests/data/tool-contracts/dev_gather_fasta_app_tool_contract.json
	python -m pbcommand.cli.examples.dev_scatter_fasta_app --emit-tool-contract > ./tests/data/tool-contracts/dev_scatter_fasta_app_tool_contract.json
	python -m pbcommand.cli.examples.dev_quick_hello_world emit-tool-contracts -o ./tests/data/tool-contracts

run-pep8:
	find pbcommand -name "*.py" -exec pep8 --ignore=E501,E265,E731,E402,W292 {} \;

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
