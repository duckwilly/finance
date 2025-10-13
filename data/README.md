# Data Directory

Synthetic CSV output from `scripts/gen_seed_data.py` lands here. The generator
creates a static snapshot under `data/seed/` and, if you enable the live mode,
a rolling append-only feed under `data/stream/`.

You can safely delete generated files; they will be recreated on the next run.
Check the README in the project root for generation parameters and tips on
sizing the dataset for demos.
