# Quickstart Options

The quickstart script now supports various configuration options to generate different sizes of simulation data and control the setup process.

## Usage

```bash
./scripts/quickstart.sh [OPTIONS]
```

## Options

- `--size SIZE`: Simulation size preset (small, medium, large)
- `--no-server`: Don't start the FastAPI server
- `--help`: Show help message
## Presets

| Preset | Individuals | Companies | Months | Use Case |
|--------|-------------|-----------|--------|----------|
| `small` | 50 | 5 | 3 | Quick testing |
| `medium` | 500 | 50 | 12 | Development, demo |
| `large` | 2000 | 200 | 24 | Production-like testing |

## Examples

### Basic usage (medium preset)
```bash
make quickstart
# or
./scripts/quickstart.sh
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

### Server Targets
- `make start`: Start development server with auto-reload
- `make start-prod`: Start production server with multiple workers

## Performance Benchmarks

Benchmarks were captured on an Apple M4 (10-core) MacBook Air, macOS 26.1,
MariaDB 11.8.3 with cached market data. A fresh market data fetch will add time.

Pipeline time = seed generation + CSV load into MariaDB.

| Preset | Individuals | Companies | Months | Seed size | Generate | Load | Pipeline |
| --- | --- | --- | --- | --- | --- | --- | --- |
| small | 50 | 5 | 3 | 0.9 MB | 0.55s | 1.14s | 1.69s |
| medium | 500 | 50 | 12 | 24.2 MB | 1.65s | 4.98s | 6.62s |
| large | 2000 | 200 | 24 | 188.0 MB | 10.14s | 38.35s | 48.49s |

Choose the appropriate preset based on your needs:
- Use `small` for quick development and smoke tests
- Use `medium` for regular development and demos
- Use `large` for performance testing and production-like scenarios
