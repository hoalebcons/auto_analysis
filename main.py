from flask import Flask, request, jsonify
from google.cloud import bigquery
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import os

# ==== FLASK APP ====
app = Flask(__name__)

# ==== CONFIG ====
BQ_KEY_PATH = os.environ.get("BQ_KEY_PATH", "Key/gg_big_query.json")
GSHEET_KEY_PATH = os.environ.get("GSHEET_KEY_PATH", "Key/google_sheet.json")

TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1pTH3minpoZMoT1xicadDF3WCSqKNugrUvplcpS9U__8/edit?gid=0"

# ==== INIT BIGQUERY CLIENT ====
bq_client = bigquery.Client.from_service_account_json(BQ_KEY_PATH)

# ==== INIT GOOGLE SHEET CLIENT ====
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_service_account_file(GSHEET_KEY_PATH, scopes=SCOPES)
gc = gspread.authorize(creds)

spreadsheet = gc.open_by_url(TARGET_SHEET_URL)


@app.route("/export", methods=["GET"])
def export_data():
    # Lấy param "day" (mặc định 7 nếu không có)
    day = request.args.get("day", default="7", type=str)

    # Query BigQuery
    query = f"""
    DECLARE day_before INT64 DEFAULT {day};

    SELECT
      DATE(DATETIME_ADD(pos.inserted_at, INTERVAL 7 HOUR)) AS date_insert,
      pos.brand,
      CASE
        WHEN pos.order_sources_name IN ('Ladipage Facebook', 'Webcake') THEN 'Facebook'
        WHEN pos.order_sources_name = 'Ladipage Tiktok' THEN 'Tiktok'
        ELSE pos.order_sources_name
      END AS channel,
      JSON_VALUE(item, '$.variation_info.display_id') AS sku,
      JSON_VALUE(item, '$.variation_info.name') AS product_name,
      price.gia_ban_daily AS daily_price,
      SUM(SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64)) AS total_quantity,
      SUM(SAFE_CAST(JSON_VALUE(item, '$.quantity') AS INT64)) * IFNULL(price.gia_ban_daily, 0) AS total_amount
    FROM `crypto-arcade-453509-i8.dtm.t1_pancake_pos_order_total` AS pos
    CROSS JOIN UNNEST(items) AS item
    LEFT JOIN (
      SELECT
        ma_sku AS sku,
        gia_ban_daily
      FROM `crypto-arcade-453509-i8.dtm.t1_bang_gia_san_pham`
    ) AS price
      ON JSON_VALUE(item, '$.variation_info.display_id') = price.sku
    WHERE DATE(pos.inserted_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL day_before DAY)
      AND DATE(pos.inserted_at) < CURRENT_DATE()
    GROUP BY
      date_insert,
      pos.brand,
      channel,
      sku,
      product_name,
      daily_price
    ORDER BY
      date_insert,
      pos.brand;
    """

    # Query và đưa về DataFrame
    history = bq_client.query(query).to_dataframe()

    # Ghi ra Google Sheet
    try:
        try:
            worksheet = spreadsheet.worksheet("data")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title="data", rows=1000, cols=20)

        worksheet.clear()
        set_with_dataframe(worksheet, history, include_index=False)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    # Trả về kết quả JSON
    return jsonify({
        "status": "success",
        "rows": len(history),
        "columns": list(history.columns),
        "preview": history.head(10).to_dict(orient="records")  # preview 10 dòng đầu
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
