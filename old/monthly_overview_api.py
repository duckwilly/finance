# monthly_overview_api.py
from __future__ import annotations

from datetime import datetime
from flask import Flask, request, jsonify
from monthly_overview import monthly_overview
from logger import log

app = Flask(__name__)

@app.get("/api/monthly-overview")
def api_monthly_overview():
    year_str = request.args.get("year")
    year = int(year_str) if year_str and year_str.isdigit() else datetime.now().year
    log.info(f"HTTP GET /api/monthly-overview?year={year}")

    try:
        rows = monthly_overview(year)  # -> list[(YYYY-MM, income, expense)]
        data = [
            {"month": m, "income": float(inc), "expense": float(exp), "net": float(inc - exp)}
            for (m, inc, exp) in rows
        ]
        return jsonify({"year": year, "data": data})
    except Exception as e:
        log.error(f"Failed to produce monthly overview for {year}: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Run:  FLASK_APP=monthly_overview_api.py flask run  (or)  python monthly_overview_api.py
    log.info("Starting Flask app monthly_overview_api")
    app.run(host="127.0.0.1", port=5000, debug=True)