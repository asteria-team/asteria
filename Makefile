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

# Format with black
.PHONY: format
format:
	black --line-length 80 src/

# Lint with flake8
.PHONY: lint
lint:
	flake8 src/

# Sort imports with isort
.PHONY: isort
isort:
	isort --line-length 80 --profile black src/

# All quality assurance measures
.PHONY: qa
qa: isort format lint