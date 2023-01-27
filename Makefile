# Automation

# ---------------------------------------------------------
# Tests
# ---------------------------------------------------------

.PHONY: test
test:
	tox --develop -e py38

# ---------------------------------------------------------
# Quality Assurance
# ---------------------------------------------------------

# Sort imports with isort
.PHONY: isort
isort:
	isort --line-length 80 --profile black src/
	isort --line-length 80 --profile black src/

.PHONY: check-isort
check-isort:
	isort --check --line-length 80 --profile black src/
	isort --check --line-length 80 --profile black src/

# Format with black
.PHONY: format
format:
	black --line-length 80 src/
	black --line-length 80 test/

.PHONY: check-format
check-format:
	black --check --line-length 80 src/
	black --check --line-length 80 test/

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
