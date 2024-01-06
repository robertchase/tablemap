NAME := $(shell basename $(PWD))
PYTHONPATH := $(HOME)/git/serializer:$(PWD)/.
VENV := $(PWD)/.env

PATH := $(VENV)/bin:$(PATH)
BIN := PATH=$(PATH) PYTHONPATH=$(PYTHONPATH) $(VENV)/bin
py := $(BIN)/python3
pip := $(py) -m pip

.PHONY: test
test:
	$(BIN)/pytest tests

.PHONY: lint
lint:
	$(BIN)/pylint $(NAME) tests

.PHONY: black
black:
	$(BIN)/black $(NAME) tests

.PHONY: bump
bump:
	$(eval TMP := $(shell mktemp tmp.setup.XXXXXX))
	@awk '$$1=="version"{split($$3,n,".");$$0=sprintf("version = %d.%d.%d",n[1],n[2],n[3]+1)}{print}' setup.cfg > $(TMP)
	@mv $(TMP) setup.cfg
	@grep version setup.cfg

.PHONY: bin
bin:
	@echo $(BIN)

.PHONY: venv
venv:
	python3 -m venv $(VENV)
	$(pip) install --upgrade pip
	$(pip) install -r requirements.txt
