# Automation

# ---------------------------------------------------------
# Tests
# ---------------------------------------------------------

.PHONY: test
test:
	tox --develop -e py38

# ---------------------------------------------------------
# Storage Preparation for Development
# ---------------------------------------------------------

# Create a data lake storage location in current directory
.PHONY: dl
dl:
	mkdir dl

# Remove the data lake storage location
.PHONY: cleandl
cleandl:
	rm -rf dl

.PHONY: backend
backend:
	python src/backend/backend.py --datalake dl/ --verbose

# ---------------------------------------------------------
# Quality Assurance
# ---------------------------------------------------------

# Sort imports with isort
.PHONY: isort
isort:
	isort --line-length 80 --profile black src/
	isort --line-length 80 --profile black test/
	isort --line-length 80 --profile black testbed/

.PHONY: check-isort
check-isort:
	isort --check --line-length 80 --profile black src/
	isort --check --line-length 80 --profile black test/
	isort --check --line-length 80 --profile black testbed/

# Format with black
.PHONY: format
format:
	black src/
	black test/
	black testbed/

.PHONY: check-format
check-format:
	black --check src/
	black --check test/
	black --check testbed/

# Lint with flake8
.PHONY: lint
lint:
	flake8 src/
	flake8 test/

.PHONY: check-lint
check-lint:
	flake8 src/
	flake8 test/

# All quality assurance measures
.PHONY: qa
qa: isort format lint

.PHONY: check
check: check-isort check-format check-lint
