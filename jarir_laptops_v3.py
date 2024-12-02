import psycopg2
from psycopg2 import pool
import requests
import json
import time
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from config import DB_CONFIG

# Start time
start_time = time.time()

# Create a connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=100,
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    dbname=DB_CONFIG["dbname"],
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"]
)

# Fetch table name from configuration
tablename = DB_CONFIG.get("tablename", "test")

# Base API URL
API_URL = "https://www.jarir.com/api/catalogv1/product/store/sa-en/category_ids/1331/aggregation/true/size/12/from/"

# Custom headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9,ar-EG;q=0.8,ar;q=0.7',
    'Cache-Control': 'no-cache',
    'Content-Type': 'application/json',
    'Connection': 'keep-alive',
    'Host': 'www.jarir.com',
    'Referer': 'https://www.jarir.com/',
    'Pragma': 'no-cache',
}

def extract_product_data(product):
    """
    Extract relevant data from a product dictionary, including the category and stock status.
    """
    current_time = datetime.utcnow() + timedelta(hours=3)
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')

    product_name = product.get('name', 'No Name Available')
    product_link = f"https://www.jarir.com/{product.get('url_key', 'No Link Available')}.html"

    specs = 'No Specifications Available'
    if 'name' in product:
        parts = product_name.split(',', 1)
        if len(parts) > 1:
            specs = parts[1].strip()
            product_name = parts[0].strip()

    new_price = product.get('jarir_final_price', product.get('price', 0))
    old_price = product.get('price', new_price)
    brand = product.get('GTM_brand', 'No Brand Available')

    gtm_cofa = product.get('GTM_cofa', '')
    if gtm_cofa and gtm_cofa != 'n/a':
        specs = f"{specs}, {gtm_cofa}" if specs != "No Specifications Available" else gtm_cofa

    category = product.get('GTM_category', 'No Category Available')
    klevu_stock_flag = product.get('klevu_stock_flag', 0)
    stock = klevu_stock_flag == 1

    return {
        "name": product_name,
        "specs": specs,
        "new_price": new_price,
        "old_price": old_price,
        "brand": brand,
        "link": product_link,
        "category": "laptop",
        "datetime": current_time_str,
        "stock": stock,
        "store": "jarir"
    }

def fetch_products():
    """
    Fetch product data from the Jarir API.
    """
    start_index = 0
    all_products = []
    all_responses = []

    with requests.Session() as session:
        session.headers.update(HEADERS)

        while True:
            try:
                url = f"{API_URL}{start_index}"
                response = session.get(url)

                if response.status_code != 200:
                    print(f"Failed to fetch data at index {start_index}, status code: {response.status_code}")
                    break

                all_responses.append(response.json())

                data = response.json()
                hits = data.get('hits', {}).get('hits', [])
                total_hits = data.get('hits', {}).get('total', 0)

                if not hits:
                    print(f"No more products found at index {start_index}. Stopping.")
                    break

                for product_data in hits:
                    product = product_data.get('_source', {})
                    all_products.append(extract_product_data(product))

                if len(all_products) >= total_hits:
                    print(f"Fetched all {total_hits} products.")
                    break

                start_index += 12

            except Exception as e:
                print(f"An error occurred at index {start_index}: {e}")
                time.sleep(5)

    save_to_postgresql(all_products)
    save_responses(all_responses)

def save_to_postgresql(products):
    """
    Save product data to PostgreSQL database using batch inserts.
    """
    try:
        conn = connection_pool.getconn()
        cursor = conn.cursor()

        insert_query = f"""
        INSERT INTO {tablename} (name, specs, new_price, old_price, link, brand, category, datetime, stock, store)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        product_data = [
            (
                product['name'],
                product['specs'],
                product['new_price'],
                product['old_price'],
                product['link'],
                product['brand'],
                product['category'],
                product['datetime'],
                product['stock'],
                product['store']
            )
            for product in products
        ]

        execute_batch(cursor, insert_query, product_data)
        conn.commit()
        print(f"Successfully saved {len(products)} products to the PostgreSQL database in table '{tablename}'.")
    
    except Exception as e:
        print(f"An error occurred while saving to the database: {e}")
        conn.rollback()
    
    finally:
        if conn:
            connection_pool.putconn(conn)

def save_responses(responses):
    """
    Save all raw responses to a JSON file.
    """
    with open('raw_responses.json', 'w', encoding='utf-8') as json_file:
        json.dump(responses, json_file, ensure_ascii=False, indent=4)

    print(f"Saved {len(responses)} raw API responses to raw_responses.json.")

# Fetch products
fetch_products()

# End time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Total time taken: {elapsed_time:.2f} seconds.")
