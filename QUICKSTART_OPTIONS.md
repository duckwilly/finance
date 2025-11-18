# Quickstart Options

The quickstart script now supports various configuration options to generate different sizes of simulation data and control the setup process.

## Usage

```bash
./scripts/quickstart.sh [OPTIONS]
```

### Windows

```bash
make -f quickstart-win.mk quickstart
```

> **WSL tip:** If you run `make quickstart` inside WSL, make sure the repo files
> have LF line endings so the `#!/usr/bin/env bash` shebangs resolve correctly.
> After pulling on Windows, run `git config core.autocrlf input` once (or clone
> fresh) and re-check out the repo so scripts like `scripts/quickstart.sh` stay
> executable on both macOS and Linux.

## Options

- `--size SIZE`: Simulation size preset (small, medium, large)
- `--months MONTHS`: Number of months to simulate (overrides preset)
- `--individuals NUM`: Number of individuals (overrides preset)
- `--companies NUM`: Number of companies (overrides preset)
- `--no-server`: Don't start the FastAPI server
- `--help`: Show help message

## Presets

| Preset | Individuals | Companies | Months | Use Case |
|--------|-------------|-----------|--------|----------|
| `small` | 50 | 5 | 3 | CI/CD, quick testing |
| `medium` | 500 | 50 | 12 | Development, demo |
| `large` | 2000 | 200 | 24 | Production-like testing |

## Examples

### Basic usage (medium preset)
```bash
make quickstart
# or
./scripts/quickstart.sh
```

### Small dataset for CI
```bash
make quickstart-ci
# or
./scripts/quickstart.sh --size small --no-server
```

### Custom configuration
```bash
./scripts/quickstart.sh --individuals 100 --companies 10 --months 6
```

### Large dataset for stress testing
```bash
make quickstart-large
# or
./scripts/quickstart.sh --size large
```

### Development without server
```bash
./scripts/quickstart.sh --size medium --no-server
```

### Start server only
```bash
# Development server with auto-reload
make start

# Production server with multiple workers
make start-prod
```

## Make Targets

### Quickstart Targets
- `make quickstart`: Default medium preset
- `make quickstart-small`: Small preset (50 individuals, 5 companies, 3 months)
- `make quickstart-medium`: Medium preset (500 individuals, 50 companies, 12 months)
- `make quickstart-large`: Large preset (2000 individuals, 200 companies, 24 months)
- `make quickstart-ci`: Small preset without server (optimized for CI)

### Server Targets
- `make start`: Start development server with auto-reload
- `make start-prod`: Start production server with multiple workers

## CI/CD Integration

For GitHub Actions or other CI systems, use the small preset without server:

```yaml
- name: Setup test environment
  run: make quickstart-ci
```

This generates a minimal dataset (50 individuals, 5 companies, 3 months) without starting the server, making it perfect for fast CI runs.

## Performance Considerations

- **Small preset**: ~2-3 minutes, ~1MB data
- **Medium preset**: ~5-10 minutes, ~50MB data  
- **Large preset**: ~15-30 minutes, ~500MB data

Choose the appropriate preset based on your needs:
- Use `small` for CI/CD and quick development
- Use `medium` for regular development and demos
- Use `large` for performance testing and production-like scenarios
