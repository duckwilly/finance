"""
Chart.js Configuration Generator
Creates chart configurations from SQL query results
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Literal, Optional, Sequence
from .config import chatbot_config


class ChartValidationError(ValueError):
    """Raised when a chart descriptor is missing required axes or has bad fields."""


class ChartGenerator:
    """Generates Chart.js configurations from data"""

    def __init__(self, color_palette: Optional[List[str]] = None):
        """
        Initialize chart generator

        Args:
            color_palette: Custom color palette (uses config default if None)
        """
        self.colors = color_palette or chatbot_config.chart_color_palette

    def generate_chart_config(
        self,
        data: List[Dict[str, Any]],
        chart_type: Optional[Literal["bar", "line", "pie", "doughnut"]] = None,
        x_field: Optional[str] = None,
        y_field: Optional[str] = None,
        title: Optional[str] = None,
        unit: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate Chart.js configuration from query results (legacy auto-detect path).

        Args:
            data: List of result rows (dicts)
            chart_type: Chart type (auto-detected if None)
            x_field: Field for x-axis labels (auto-detected if None)
            y_field: Field for y-axis values (auto-detected if None)
            title: Chart title
            unit: Optional unit hint (e.g., 'currency') to force currency formatting
        """
        if not data:
            return self._empty_chart_config()

        # Auto-detect fields if not provided
        if not x_field or not y_field:
            x_field, y_field = self._detect_fields(data[0])

        # Auto-detect chart type if not provided
        if not chart_type:
            chart_type = self._detect_chart_type(data, y_field)

        # Extract labels and values
        labels = [str(row.get(x_field, "")) for row in data]
        values = [float(row.get(y_field, 0)) for row in data]

        # Determine if values are currency
        is_currency = unit == "currency" or self._is_currency_field(y_field)

        # Build datasets
        if chart_type in ["pie", "doughnut"]:
            datasets = [{
                "data": values,
                "backgroundColor": self.colors[:len(values)],
                "borderColor": "#ffffff",
                "borderWidth": 2
            }]
        else:
            datasets = [{
                "label": y_field.replace("_", " ").title(),
                "data": values,
                "backgroundColor": self.colors[0],
                "borderColor": self.colors[0],
                "borderWidth": 2,
                "tension": 0.4 if chart_type == "line" else 0
            }]

        # Build chart config
        config = {
            "type": chart_type,
            "data": {
                "labels": labels,
                "datasets": datasets
            },
            "options": self._build_options(chart_type, is_currency, title)
        }

        return config

    def generate_multi_series_chart(
        self,
        data: List[Dict[str, Any]],
        x_field: str,
        y_fields: List[str],
        chart_type: Literal["bar", "line"] = "bar",
        title: Optional[str] = None,
        unit: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate multi-series chart (e.g., income vs expenses by month)

        Args:
            data: List of result rows
            x_field: Field for x-axis (e.g., 'month')
            y_fields: List of fields for y-axis series (e.g., ['income', 'expenses'])
            chart_type: Chart type
            title: Chart title

        Returns:
            Chart.js configuration dict
        """
        if not data:
            return self._empty_chart_config()

        labels = [str(row.get(x_field, "")) for row in data]

        datasets = []
        for i, field in enumerate(y_fields):
            values = [float(row.get(field, 0)) for row in data]
            color = self.colors[i % len(self.colors)]

            datasets.append({
                "label": field.replace("_", " ").title(),
                "data": values,
                "backgroundColor": color,
                "borderColor": color,
                "borderWidth": 2,
                "tension": 0.4 if chart_type == "line" else 0
            })

        is_currency = unit == "currency" or any(self._is_currency_field(f) for f in y_fields)

        config = {
            "type": chart_type,
            "data": {
                "labels": labels,
                "datasets": datasets
            },
            "options": self._build_options(chart_type, is_currency, title)
        }

        return config

    def generate_chart_config_enforced(
        self,
        data: List[Dict[str, Any]],
        descriptor: Dict[str, Any],
        fallback_chart_type: Optional[str] = None,
        title: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a chart honoring explicit descriptor fields with validation.

        Raises ChartValidationError when required axes are missing or mismatched.
        Falls back to legacy generation when no descriptor fields are provided.
        """
        if not data:
            return self._empty_chart_config()

        chart_type = descriptor.get("chart_type") or fallback_chart_type or "bar"
        x_axis = descriptor.get("x_axis")
        y_axis = descriptor.get("y_axis")
        stack_by = descriptor.get("stack_by")
        unit = descriptor.get("unit")
        sort_direction = (descriptor.get("sort") or "").lower()

        has_descriptor_fields = any([chart_type, x_axis, y_axis, stack_by, unit, sort_direction])
        if not has_descriptor_fields:
            return self.generate_chart_config(
                data,
                chart_type=chart_type,  # may be None -> auto detect
                title=title,
            )

        if chart_type not in {"bar", "line", "pie", "doughnut"}:
            raise ChartValidationError("chart_type must be one of bar|line|pie|doughnut")

        if not x_axis and chart_type in {"bar", "line"}:
            raise ChartValidationError("x_axis is required for bar and line charts")
        if not x_axis and chart_type in {"pie", "doughnut"}:
            raise ChartValidationError("x_axis is required for pie and doughnut charts")
        if not y_axis:
            raise ChartValidationError("y_axis is required to plot values")
        if chart_type in {"pie", "doughnut"} and stack_by:
            raise ChartValidationError("stack_by is not supported for pie/doughnut charts")

        y_fields: Sequence[str] = y_axis if isinstance(y_axis, (list, tuple)) else [y_axis]
        if chart_type in {"pie", "doughnut"} and len(y_fields) != 1:
            raise ChartValidationError("pie/doughnut charts require a single y_axis value")

        # Validate requested fields exist in data
        sample = data[0]
        missing_fields = [
            field for field in [x_axis, stack_by, *y_fields] if field and field not in sample
        ]
        if missing_fields:
            raise ChartValidationError(f"Fields not found in result set: {', '.join(missing_fields)}")

        rows = list(data)
        if sort_direction in {"asc", "desc"} and len(y_fields) == 1 and not stack_by:
            key_field = y_fields[0]
            reverse = sort_direction == "desc"
            rows = sorted(rows, key=lambda r: float(r.get(key_field, 0) or 0), reverse=reverse)

        if stack_by:
            return self._generate_stacked_chart(rows, x_axis, y_fields[0], stack_by, chart_type, title, unit)

        if len(y_fields) > 1:
            if chart_type not in {"bar", "line"}:
                raise ChartValidationError("Multiple y_axis values are only supported for bar/line charts")
            return self.generate_multi_series_chart(
                rows,
                x_field=x_axis,
                y_fields=list(y_fields),
                chart_type=chart_type or "bar",
                title=title,
                unit=unit,
            )

        return self.generate_chart_config(
            rows,
            chart_type=chart_type,
            x_field=x_axis,
            y_field=y_fields[0],
            title=title,
            unit=unit,
        )

    def _generate_stacked_chart(
        self,
        data: List[Dict[str, Any]],
        x_field: str,
        y_field: str,
        stack_by: str,
        chart_type: Literal["bar", "line"],
        title: Optional[str],
        unit: Optional[str],
    ) -> Dict[str, Any]:
        if chart_type not in {"bar", "line"}:
            raise ChartValidationError("stack_by is only supported for bar/line charts")

        labels: list[str] = []
        series_map: dict[str, list[float]] = {}

        # Preserve the original order of labels as they appear
        for row in data:
            label_val = str(row.get(x_field, ""))
            if label_val not in labels:
                labels.append(label_val)

        stack_values: dict[str, dict[str, float]] = {}
        for row in data:
            label_val = str(row.get(x_field, ""))
            stack_val = str(row.get(stack_by, ""))
            stack_values.setdefault(stack_val, {})
            stack_values[stack_val][label_val] = float(row.get(y_field, 0) or 0)

        for stack_val, label_map in stack_values.items():
            series_map[stack_val] = [label_map.get(label, 0.0) for label in labels]

        datasets = []
        is_currency = unit == "currency" or self._is_currency_field(y_field)

        for idx, (series_name, values) in enumerate(series_map.items()):
            color = self.colors[idx % len(self.colors)]
            datasets.append(
                {
                    "label": series_name.replace("_", " ").title(),
                    "data": values,
                    "backgroundColor": color,
                    "borderColor": color,
                    "borderWidth": 2,
                    "tension": 0.4 if chart_type == "line" else 0,
                    "stack": "stacked",
                }
            )

        return {
            "type": chart_type,
            "data": {
                "labels": labels,
                "datasets": datasets,
            },
            "options": self._build_options(chart_type, is_currency, title, stacked=True),
        }

    def _detect_fields(self, row: Dict[str, Any]) -> tuple[str, str]:
        """Auto-detect x and y fields from data structure"""
        keys = list(row.keys())

        # Look for common label fields
        label_candidates = ["category", "month", "year", "name", "type", "channel"]
        x_field = next((k for k in keys if k.lower() in label_candidates), keys[0])

        # Look for common value fields
        value_candidates = ["total", "amount", "value", "sum", "count"]
        y_field = next((k for k in keys if k.lower() in value_candidates), keys[-1])

        return x_field, y_field

    def _detect_chart_type(self, data: List[Dict], y_field: str) -> str:
        """Auto-detect appropriate chart type"""
        num_rows = len(data)

        # Pie/doughnut for small categorical data
        if num_rows <= 7:
            return "doughnut"

        # Line chart for time series
        first_row = data[0]
        if any(k in str(first_row.keys()).lower() for k in ["month", "year", "date", "day"]):
            return "line"

        # Default to bar
        return "bar"

    def _is_currency_field(self, field_name: str) -> bool:
        """Check if field represents currency"""
        currency_keywords = ["amount", "total", "sum", "income", "expense", "revenue", "cost", "price"]
        return any(kw in field_name.lower() for kw in currency_keywords)

    def _build_options(
        self,
        chart_type: str,
        is_currency: bool,
        title: Optional[str],
        stacked: bool = False,
    ) -> Dict[str, Any]:
        """Build Chart.js options configuration"""

        options = {
            "responsive": True,
            "maintainAspectRatio": True,
            "plugins": {
                "legend": {
                    "display": True,
                    "position": "top"
                }
            }
        }

        # Add title if provided
        if title:
            options["plugins"]["title"] = {
                "display": True,
                "text": title,
                "font": {"size": 16}
            }

        # Add axes for bar/line charts
        if chart_type in ["bar", "line"]:
            y_axis_config = {
                "beginAtZero": True,
                "ticks": {}
            }
            if stacked:
                y_axis_config["stacked"] = True
                x_axis_config = {"stacked": True}

            # Currency formatting
            if is_currency:
                y_axis_config["ticks"]["callback"] = "##CURRENCY_CALLBACK##"

            options["scales"] = {
                "y": y_axis_config
            }
            if stacked:
                options["scales"]["x"] = x_axis_config

        # Tooltip configuration
        if is_currency:
            options["plugins"]["tooltip"] = {
                "callbacks": {
                    "label": "##CURRENCY_TOOLTIP##"
                }
            }

        return options

    def _empty_chart_config(self) -> Dict[str, Any]:
        """Return empty chart configuration"""
        return {
            "type": "bar",
            "data": {
                "labels": [],
                "datasets": []
            },
            "options": {
                "responsive": True,
                "plugins": {
                    "legend": {"display": False}
                }
            }
        }

    def format_for_frontend(self, config: Dict[str, Any]) -> str:
        """
        Format chart config for frontend with JS function callbacks

        Args:
            config: Chart.js config dict

        Returns:
            JSON string with function callbacks preserved
        """
        # Convert to JSON
        json_str = json.dumps(config, indent=2)

        # Replace currency callback placeholders with actual JS functions
        json_str = json_str.replace(
            '"##CURRENCY_CALLBACK##"',
            """function(value) {
                return '€' + value.toLocaleString('de-DE', {minimumFractionDigits: 2, maximumFractionDigits: 2});
            }"""
        )

        json_str = json_str.replace(
            '"##CURRENCY_TOOLTIP##"',
            """function(context) {
                let label = context.dataset.label || '';
                if (label) label += ': ';
                label += '€' + context.parsed.y.toLocaleString('de-DE', {minimumFractionDigits: 2, maximumFractionDigits: 2});
                return label;
            }"""
        )

        return json_str
