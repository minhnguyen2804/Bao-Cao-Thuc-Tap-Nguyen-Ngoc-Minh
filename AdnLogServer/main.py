#pip install fastapi uvicorn clickhouse-connect
#chay app: uvicorn main:app --host 0.0.0.0 --port 8000
#truy van: http://localhost:8000/countforcampaign?start_date=2024-12-11&end_date=2024-12-12
from fastapi import FastAPI, Query, HTTPException
from clickhouse_connect import get_client
from datetime import date
from typing import Optional

app = FastAPI()
def get_clickhouse_client():
    try:
        client = get_client(
            host="localhost",
            port=8123,
            user="default",
            password="123456",
            database="default"
        )
        return client
    except Exception as e:
        print("⚠️ Không kết nối được CSDL:", e)
        return None

@app.get("/countforbanner")
def get_estimated_users(
    start_date: Optional[date] = Query(..., description="YYYY-MM-DD"),
    end_date: Optional[date] = Query(..., description="YYYY-MM-DD")
):
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")
    where_clause = ""
    if start_date and end_date:
        where_clause = f"WHERE event_date BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        where_clause = f"WHERE event_date = '{start_date}'"
    elif end_date:
        where_clause = f"WHERE event_date = '{end_date}'"
    query = f"""
        SELECT banner_id, click_or_view, uniqHLL12(guid) AS estimated_user_count
        FROM adnlog_raw_v2
        {where_clause}
        GROUP BY banner_id, click_or_view
        ORDER BY banner_id
    """
    client = get_clickhouse_client()
    result = client.query(query)
    return result.result_rows

@app.get("/countforcampaign")
def get_estimated_users(
    start_date: Optional[date] = Query(..., description="YYYY-MM-DD"),
    end_date: Optional[date] = Query(..., description="YYYY-MM-DD")
):
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")
    where_clause = ""
    if start_date and end_date:
        where_clause = f"WHERE event_date BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        where_clause = f"WHERE event_date = '{start_date}'"
    elif end_date:
        where_clause = f"WHERE event_date = '{end_date}'"
    query = f"""
        SELECT campaign_id, click_or_view, uniqHLL12(guid) AS estimated_user_count
        FROM adnlog_raw_campaign
        {where_clause}
        GROUP BY campaign_id, click_or_view
        ORDER BY campaign_id
    """
    client = get_clickhouse_client()
    result = client.query(query)
    return result.result_rows