"""ETL utilities for CSV ingest and enrichment rules."""

DEFAULT_SECTION_RULES = {
    "salary": "income",
    "rent": "expense",
    "transfer": "transfer",
}


def resolve_section(category: str, default: str = "expense") -> str:
    """Map a category to a canonical section name."""

    key = category.lower().strip()
    return DEFAULT_SECTION_RULES.get(key, default)
