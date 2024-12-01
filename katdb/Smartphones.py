import psycopg2
from psycopg2 import pool
import requests
import csv
import json
import time
from psycopg2.extras import execute_batch
from datetime import datetime
import pytz

# Start time
start_time = time.time()

# Database connection parameters
DB_HOST = "ah-data-lab.duckdns.org"
DB_PORT = "35432"
DB_NAME = "moaz_db"
DB_USER = "moaz"
DB_PASSWORD = "moaz"

# Create a connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,  # Minimum number of connections
    maxconn=100, # Maximum number of connections
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

# Base API URL
API_URL = "https://www.jarir.com/api/catalogv1/product/store/sa-en/category_ids/1008/aggregation/true/size/24/from/"

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

def get_saudi_arabia_time():
    """Get current time in Saudi Arabia timezone."""
    saudi_tz = pytz.timezone('Asia/Riyadh')
    return datetime.now(saudi_tz).strftime('%Y-%m-%d %H:%M:%S')

def extract_product_data(product):
    """
    Extract relevant data from a product dictionary, including the category.
    """
    product_name = product.get('name', 'No Name Available')
    product_link = f"https://www.jarir.com/{product.get('url_key', 'No Link Available')}.html"

    # Handling specs extraction
    specs = 'No Specifications Available'
    if 'name' in product:
        # Try to split the name into product name and specs
        parts = product_name.split(',', 1)
        if len(parts) > 1:
            specs = parts[1].strip()
            product_name = parts[0].strip()  # Remove specs from product name

    # Prices
    new_price = product.get('jarir_final_price', product.get('price', 0))
    old_price = product.get('price', new_price)
    brand = product.get('GTM_brand', 'No Brand Available')

    # Extract GTM_cofa
    gtm_cofa = product.get('GTM_cofa', '')

    # Only add GTM_cofa to specs if it exists and is valid
    if gtm_cofa and gtm_cofa != 'n/a':
        specs = f"{specs}, {gtm_cofa}" if specs != "No Specifications Available" else gtm_cofa

    # Extract the category (GTM_category)
    category = product.get('GTM_category', 'No Category Available')

    # Get the current time in Saudi Arabia
    saudi_time = get_saudi_arabia_time()

    return {
        "name": product_name,
        "specs": specs,  # Specs now include GTM_cofa only if it exists
        "new_price": new_price,
        "old_price": old_price,
        "brand": brand,
        "link": product_link,
        "category": category,
        "fetched_at": saudi_time  # Add the Saudi Arabia timestamp
    }

def fetch_products():
    """Fetch product data from the Jarir API."""
    start_index = 0
    all_products = []
    all_responses = []

    with requests.Session() as session:
        session.headers.update(HEADERS)

        while True:
            try:
                # Construct the API URL with pagination
                url = f"{API_URL}{start_index}"
                response = session.get(url)

                if response.status_code != 200:
                    print(f"Failed to fetch data at index {start_index}, status code: {response.status_code}")
                    break

                # Save the raw response
                all_responses.append(response.json())

                # Parse JSON response
                data = response.json()
                hits = data.get('hits', {}).get('hits', [])
                total_hits = data.get('hits', {}).get('total', 0)

                if not hits:
                    print(f"No more products found at index {start_index}. Stopping.")
                    break

                # Process each product
                for product_data in hits:
                    product = product_data.get('_source', {})
                    all_products.append(extract_product_data(product))

                # Break if we've fetched all products
                if len(all_products) >= total_hits:
                    print(f"Fetched all {total_hits} products.")
                    break

                # Move to the next page
                start_index += 12

            except Exception as e:
                print(f"An error occurred at index {start_index}: {e}")
                time.sleep(5)  # Retry after a delay

    # Save data to PostgreSQL
    if all_products:  # Ensure there is data before saving
        save_to_postgresql(all_products)
    save_responses(all_responses)

def save_to_postgresql(products):
    """
    Save product data to PostgreSQL database using batch inserts.
    """
    try:
        # Get a connection from the pool
        conn = connection_pool.getconn()
        cursor = conn.cursor()

        # Prepare the insert statement (updated to include category)
        insert_query = """
        INSERT INTO productstestadd (name, specs, new_price, old_price, link, brand, category, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare the data for batch insert (updated to include category and time)
        product_data = [(product['name'], product['specs'], product['new_price'], product['old_price'], product['link'], product['brand'], product['category'], product['fetched_at']) for product in products]

        # Batch insert
        execute_batch(cursor, insert_query, product_data)

        # Commit the changes
        conn.commit()

        # Return the connection to the pool
        connection_pool.putconn(conn)

        print(f"Successfully saved {len(products)} products to the PostgreSQL database.")

    except Exception as e:
        print(f"An error occurred while saving to the database: {e}")

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

# Calculate and print the total time taken
elapsed_time = end_time - start_time
print(f"Total time taken: {elapsed_time:.2f} seconds.")