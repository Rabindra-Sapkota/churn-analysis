#!/usr/local/bin/python3.6

from flask import Flask, jsonify, request
from find_churn import get_churn_trend, get_churn_info_month

app = Flask(__name__)

@app.route("/churn_trend/<product_name>", methods=["GET"])
def churn(product_name):
    result = get_churn_trend(product_name)
    return jsonify(result.to_dict(orient='records'))


@app.route("/churn_distribution/<year>/<month>", methods=["GET"])
def churn_distribution(year, month):
    result = get_churn_info_month(year,month)
    return jsonify(result.to_dict(orient='records'))


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
