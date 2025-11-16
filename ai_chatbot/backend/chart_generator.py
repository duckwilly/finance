"""
Chart.js Configuration Generator
Creates chart configurations from SQL query results
"""
import json
from typing import Dict, Any, List, Optional, Literal
from .config import chatbot_config


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
        title: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate Chart.js configuration from query results

        Args:
            data: List of result rows (dicts)
            chart_type: Chart type (auto-detected if None)
            x_field: Field for x-axis labels (auto-detected if None)
            y_field: Field for y-axis values (auto-detected if None)
            title: Chart title

        Returns:
            Chart.js configuration dict
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
        is_currency = self._is_currency_field(y_field)

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
        title: Optional[str] = None
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

        is_currency = any(self._is_currency_field(f) for f in y_fields)

        config = {
            "type": chart_type,
            "data": {
                "labels": labels,
                "datasets": datasets
            },
            "options": self._build_options(chart_type, is_currency, title)
        }

        return config

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
        title: Optional[str]
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

            # Currency formatting
            if is_currency:
                y_axis_config["ticks"]["callback"] = "##CURRENCY_CALLBACK##"

            options["scales"] = {
                "y": y_axis_config
            }

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
