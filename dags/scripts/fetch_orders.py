import requests
import pandas as pd
import os
from datetime import datetime
import random

def fetch_orders():
    print("Mengambil data dari API Orders...")
    url = "http://96.9.212.102:8000/orders"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()

    orders = data["orders"]

    parsed_data = []

    for order in orders:

        order_id = str(order.get("order_id"))
        customer_id = str(order.get("user_id"))

        order_date = datetime.now().strftime("%Y-%m-%d")

        products = order.get("products", [])

        for product in products:

            quantity = 1

            unit_price = round(
                (product.get("product_id", 1) % 50) + 1,
                2
            )

            total_price = round(unit_price * quantity, 2)

            discount = round(random.uniform(0, 5), 2)

            tax = round(total_price * 0.1, 2)

            parsed_data.append({

                "order_id": order_id,

                "customer_id": customer_id,

                "product_id": str(product.get("product_id")),

                "product_name": str(product.get("product_name")),

                "category": str(product.get("department")),

                "quantity": quantity,

                "unit_price": unit_price,

                "total_price": total_price,

                "discount": discount,

                "tax": tax,

                "status": "completed",

                "payment_method": random.choice([
                    "credit_card",
                    "bank_transfer",
                    "e_wallet"
                ]),

                "shipping_city": random.choice([
                    "Jakarta",
                    "Surabaya",
                    "Bandung",
                    "Medan"
                ]),

                "shipping_country": "Indonesia",

                "reordered": int(
                    product.get("reordered", 0)
                ),

                "order_date": order_date,

                "updated_at": datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),

                "ingested_at": datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            })

    df = pd.DataFrame(parsed_data)

    output_dir = "/opt/airflow/data_lake/orders"

    os.makedirs(output_dir, exist_ok=True)

    output_path = (
        f"{output_dir}/orders_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    )

    df.to_parquet(output_path, index=False)

    print(f"✅ Berhasil menyimpan {len(df)} rows")


if __name__ == "__main__":
    fetch_orders()