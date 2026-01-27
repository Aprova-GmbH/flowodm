# Publishing Guide for FlowODM

This document contains all instructions for publishing and maintaining the FlowODM package.

Repository: https://github.com/vykhand/flowodm

## 1. Publishing to PyPI

### 1.1 Publishing a Release

Publishing is automated via GitHub Actions. Here's how to release:

```bash
cd /Users/vykhand/DEV/flowodm

# Make sure all tests pass (unit tests only - no Kafka needed!)
uv run pytest -m unit --cov=flowodm --cov-report=term-missing

# Make sure linting passes
uv run black --check src tests
uv run ruff check src tests

# Build package locally to verify
uv run python -m build
uv run twine check dist/*

# If all checks pass, create and push a version tag
git tag v0.1.0
git push origin v0.1.0
```

**What happens next:**
1. GitHub Actions workflow `.github/workflows/publish.yml` triggers automatically
2. Package is built
3. Package is uploaded to PyPI
4. GitHub Release is created with release notes

**Verify publication:**
- Check PyPI: https://pypi.org/project/flowodm/
- Install and test: `pip install flowodm`

### 1.2 Manual Publishing (Fallback)

If GitHub Actions fails, you can publish manually:

```bash
cd /Users/vykhand/DEV/flowodm

# Build the package
uv run python -m build

# Upload to PyPI
uv run twine upload dist/*
# Enter your PyPI API token when prompted (username: __token__)
```

---

## 2. Publishing Documentation

Documentation automatically rebuilds on ReadTheDocs with every push to `main` via webhook.

- Documentation URL: https://flowodm.readthedocs.io
- Build status: https://readthedocs.org/projects/flowodm/builds/
- Configuration: `.readthedocs.yaml` in repository root (if added)

---

## 3. Creating New Versions

Follow this process for each new release:

### 3.1 Update Version Number

Update version in **three files**:

**File 1: `pyproject.toml`**
```toml
[project]
name = "flowodm"
version = "0.2.0"  # <- Update this
```

**File 2: `src/flowodm/__init__.py`**
```python
__version__ = "0.2.0"  # <- Update this
```

**File 3: `docs/conf.py`**
```python
release = "0.2.0"  # <- Update this
```

### 3.2 Update CHANGELOG.md

Follow [Keep a Changelog](https://keepachangelog.com/) format:

```markdown
# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-02-15

### Added
- Support for Kafka Streams integration
- New ConsumerLoop pattern for microservices

### Changed
- Improved schema validation performance

### Fixed
- Fixed nullable type handling in Avro schemas

## [0.1.0] - 2026-01-27

### Added
- Initial release
- Pydantic v2 native Kafka ODM
- Dual sync/async support
- Schema Registry integration
```

### 3.3 Commit Changes

```bash
cd /Users/vykhand/DEV/flowodm

git add pyproject.toml src/flowodm/__init__.py docs/conf.py CHANGELOG.md
git commit -m "Bump version to 0.2.0"
git push origin main
```

### 3.4 Run Tests

```bash
# Run full unit test suite (no Kafka required!)
uv run pytest -m unit --cov=flowodm --cov-report=term-missing -v

# Run linters
uv run black --check src tests
uv run ruff check src tests

# Build and check package
uv run python -m build
uv run twine check dist/*
```

### 3.5 Create and Push Tag

```bash
# Create annotated tag with release notes
git tag -a v0.2.0 -m "$(cat <<'EOF'
Release v0.2.0

Added:
- Support for Kafka Streams integration
- New ConsumerLoop pattern for microservices

Changed:
- Improved schema validation performance

Fixed:
- Fixed nullable type handling in Avro schemas
EOF
)"

# Push tag to trigger automated publishing
git push origin v0.2.0
```

### 3.6 Verify Release

1. **GitHub Actions:** Check https://github.com/vykhand/flowodm/actions
2. **PyPI:** Verify at https://pypi.org/project/flowodm/
3. **GitHub Release:** Check https://github.com/vykhand/flowodm/releases
4. **Documentation:** Verify at https://flowodm.readthedocs.io

### 3.7 Test Installation

```bash
# In a fresh virtual environment
python -m venv test-env
source test-env/bin/activate  # On Windows: test-env\Scripts\activate
pip install --upgrade flowodm

# Test import
python -c "from flowodm import FlowBaseModel; print(FlowBaseModel.__doc__)"

# Deactivate
deactivate
```

---

## 4. Semantic Versioning Guide

Follow [Semantic Versioning](https://semver.org/): `MAJOR.MINOR.PATCH`

- **PATCH** (0.1.0 → 0.1.1): Bug fixes, no API changes
- **MINOR** (0.1.0 → 0.2.0): New features, backward compatible
- **MAJOR** (0.9.0 → 1.0.0): Breaking changes, incompatible API changes

**Examples:**
- Fix typo in docstring: `0.1.0 → 0.1.1` (PATCH)
- Add new optional parameter: `0.1.0 → 0.2.0` (MINOR)
- Remove deprecated function: `0.9.0 → 1.0.0` (MAJOR)

---

## 5. Pre-Release Checklist

Before creating any release, verify:

- [ ] All unit tests pass (`pytest -m unit`)
- [ ] Code coverage ≥95% (`pytest --cov`)
- [ ] Linting passes (`black --check`, `ruff check`)
- [ ] Documentation builds (`sphinx-build -W -b html docs docs/_build/html`)
- [ ] CHANGELOG.md updated
- [ ] Version bumped in all 3 files
- [ ] README examples still work
- [ ] Package builds successfully (`python -m build`)
- [ ] Package metadata is valid (`twine check dist/*`)

---

## 6. Troubleshooting

### GitHub Actions Fails
- Check workflow logs: https://github.com/vykhand/flowodm/actions
- Verify `PYPI_API_TOKEN` secret is set correctly
- Ensure PyPI token has correct scope

### PyPI Upload Fails
- Verify you're not re-uploading the same version
- Check token permissions
- Try manual upload with `twine upload dist/*`

### Documentation Build Fails
- Check ReadTheDocs build logs
- Verify all dependencies in `pyproject.toml` under `[project.optional-dependencies]`
- Test local build: `cd docs && sphinx-build -W -b html . _build/html`

### Tests Fail After Version Bump
- Ensure version string in `__init__.py` matches `pyproject.toml`
- Clear pytest cache: `rm -rf .pytest_cache`
- Reinstall package: `uv pip install -e .`

---

## 7. Maintaining Documentation Versions

ReadTheDocs can host multiple versions:

1. **Tag-based versions:** Every git tag creates a new doc version
2. **Enable in ReadTheDocs:**
   - Go to https://readthedocs.org/projects/flowodm/versions/
   - Activate versions you want to publish (e.g., `v0.1.0`, `v0.2.0`)
3. **Set default version:** Choose which version shows by default (usually `latest` or `stable`)

---

## 8. Testing Notes

**Important:** FlowODM unit tests use mocks and do NOT require Kafka or Schema Registry!

- All 34 unit tests run with `pytest -m unit` using mocked connections
- No Docker containers needed for CI/CD
- Integration tests (if added) would require real Kafka, but are optional

This makes the release process much faster and simpler than libraries requiring external services.

---

## Contact

For questions about publishing or releases:
- Email: andrey@aprova.ai
- GitHub Issues: https://github.com/vykhand/flowodm/issues