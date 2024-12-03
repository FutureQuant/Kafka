# fastapi_api.py

from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

# PostgreSQL configuration
connection = psycopg2.connect(
    database="your_database",
    user="your_user",
    password="password",
    host="localhost",
    port="5432"
)

@app.get("/orders")
def get_orders():
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM orders")
    orders = cursor.fetchall()
    cursor.close()
    return orders

@app.get("/orders/{order_id}")
def get_order(order_id: str):
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
    order = cursor.fetchone()
    cursor.close()
    if order:
        return order
    return {"error": "Order not found"}

