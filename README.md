## Asteria

_Asteria_ is an (optionally) automated machine learing operations (MLOps) pipeline.

### Contents

The contents of this repository are organized as follows:

- `src/`: Source code for tool implementations.
- `test/`: Unit tests for tool implementations; integration tests for higher-order pipeline functionality.
- `tools/`: Tool definitions.

### Development

This section describes the development process.

**Source Formatting**

We format source with `black`.

```bash
make format
```

**Source Linting**

We lint source with `flake8`.

```bash
make lint
```

**Import Sorting**

We sort imports with `isort`.

```bash
make isort
```

**Quality Assurance**

Execute all quality assurance measures (`format`, `lint`, `isort`) with:

```bash
make qa
```

**Tests**

We execute unit and integration tests with `pytest`.

```bash
make test
```