from flask import Flask, render_template, jsonify, request
#from pyngrok import ngrok
import mysql.connector
import statistics

#ngrok.set_auth_token("3AKEvX4qI21UaAB4cX3yxgWk7FM_41AneqQLpQT1pi2P2jNzR")
app = Flask(__name__)

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "",
    "database": "cs179g"
}

# Sidebar structure
CATEGORIES = {
    "Time-of-Day": {
        "Hourly Crimes": "hourly_crimes",
        "Time Period Crimes": "time_period_crimes"
    },
    "Seasonal / Monthly / Holiday": {
        "Monthly Crimes": "monthly_crimes",
        "Season Crimes": "season_crimes",
        "Holiday vs Non-Holiday": "holiday_vs_nonholiday",
        "Christmas": "christmas_by_type",
        "Thanksgiving": "thanksgiving_by_type",
        "Halloween": "halloween_by_type"
    },
    "Location-Based / Spatial": {
        "Crimes by Location": "crimes_by_location",
        "Crimes by Location & Type": "crimes_by_location_and_type",
        "Community Area Crimes": "community_area_crimes",
        "Airport Theft Comparison": "airport_theft_count_comparison",
        "Downtown vs Residential": "downtown_vs_residential_theft_robbery",
        "Transit vs Commercial": "transit_vs_commercial_robbery_count",
        "Theft by Location": "theft_by_location",
        "Sports Location Crimes": "sport_location_crimes",
        "Sports Location Crimes by Type": "sport_location_crimes_by_type"
    },
    "Long-Term Trends": {
        "Yearly Crimes": "yearly_crimes",
        "Great Recession by Type": "great_recession_by_type"
    }
}

def query_table(table_name):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {table_name}")
    data = cursor.fetchall()
    conn.close()
    return data

@app.route("/")
def index():
    return render_template("index.html", categories=CATEGORIES)

@app.route("/api/data")
def get_data():
    table = request.args.get("table")
    data = query_table(table)

    # Example basic statistic (total count if column exists)
    stats = {}
    if data and "count" in data[0]:
        counts = [row["count"] for row in data]
        stats["total"] = sum(counts)
        stats["average"] = round(statistics.mean(counts), 2)

    return jsonify({
        "data": data,
        "stats": stats
    })

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", default=5000, type=int)
    args = parser.parse_args()

    app.run(host=args.host, port=args.port, debug=True)
