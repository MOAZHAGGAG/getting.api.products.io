import asyncio
import aiohttp
import json
import time
from datetime import datetime
import pytz  # For timezone handling
import asyncpg
import re  # For regex-based link filtering

# Start time
start_time = time.time()

# Database connection parameters
DB_HOST = "ah-data-lab.duckdns.org"
DB_PORT = "35432"
DB_NAME = "moaz_db"
DB_USER = "moaz"
DB_PASSWORD = "moaz"

# Base API URL
API_URL = "https://www.jarir.com/api/catalogv1/product/store/sa-en/category_ids/1008/aggregation/true/size/10000/from/"

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

# Define a function to filter products by their link
def filter_product_by_link(link):
    """
    Filter products by their link. 
    Modify this function to match your desired criteria for filtering links.
    """
    # Regex pattern to filter links that contain 'smartphone' or 'laptop' (example)
    pattern = r'(smartphone|laptop)'
    
    if re.search(pattern, link, re.IGNORECASE):
        return True
    return False

async def extract_product_data(product):
    """
    Extract relevant data from a product dictionary, including the category.
    """
    product_name = product.get('name', 'No Name Available')
    product_link = f"https://www.jarir.com/{product.get('url_key', 'No Link Available')}.html"

    # Handling specs extraction
    specs = 'No Specifications Available'
    if 'name' in product:
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

    # Get the current Saudi Arabian time
    sa_timezone = pytz.timezone('Asia/Riyadh')
    current_sa_time = datetime.now(sa_timezone)  # Timestamp for Saudi Arabia

    return {
        "name": product_name,
        "specs": specs,
        "new_price": new_price,
        "old_price": old_price,
        "brand": brand,
        "link": product_link,
        "category": "smartphones",
        "timestamp": current_sa_time  # Add timestamp to product data
    }

async def fetch_products():
    """
    Fetch product data from the Jarir API.
    """
    start_index = 0
    all_products = []
    all_responses = []

    # Set to track unique products to avoid duplicates
    seen_products = set()

    async with aiohttp.ClientSession() as session:
        session.headers.update(HEADERS)

        while True:
            try:
                url = f"{API_URL}{start_index}"
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"Failed to fetch data at index {start_index}, status code: {response.status}")
                        break

                    all_responses.append(await response.json())
                    data = await response.json()
                    hits = data.get('hits', {}).get('hits', [])
                    total_hits = data.get('hits', {}).get('total', 0)

                    if not hits:
                        print(f"No more products found at index {start_index}. Stopping.")
                        break

                    for product_data in hits:
                        product = product_data.get('_source', {})
                        product_data = await extract_product_data(product)

                        if filter_product_by_link(product_data['link']):
                            product_key = (
                                product_data['name'],
                                product_data['specs'],
                                product_data['new_price'],
                                product_data['old_price'],
                                product_data['link']
                            )

                            if product_key not in seen_products:
                                seen_products.add(product_key)
                                all_products.append(product_data)

                    if len(all_products) >= total_hits:
                        print(f"Fetched all {total_hits} products.")
                        break

                    start_index += 12

            except Exception as e:
                print(f"An error occurred at index {start_index}: {e}")
                time.sleep(5)

    await save_to_postgresql(all_products)
    save_responses(all_responses)

async def save_to_postgresql(products):
    """
    Save filtered product data to PostgreSQL database using asyncpg.
    """
    try:
        conn = await asyncpg.connect(
            user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST, port=DB_PORT
        )

        insert_query = """
        INSERT INTO productstestt (name, specs, new_price, old_price, link, brand, category, datetime)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (link) DO NOTHING
        """

        await conn.executemany(insert_query, [
            (
                product['name'],
                product['specs'],
                product['new_price'],
                product['old_price'],
                product['link'],
                product['brand'],
                product['category'],
                product['timestamp']  # Pass the timestamp here
            )
            for product in products
        ])

        await conn.close()
        print(f"Successfully saved {len(products)} filtered products to the PostgreSQL database.")

    except Exception as e:
        print(f"An error occurred while saving to the database: {e}")

def save_responses(responses):
    """
    Save all raw responses to a JSON file.
    """
    with open('raw_responses.json', 'w', encoding='utf-8') as json_file:
        json.dump(responses, json_file, ensure_ascii=False, indent=4)

    print(f"Saved {len(responses)} raw API responses to raw_responses.json.")

# Run the asynchronous function properly using asyncio
async def main():
    await fetch_products()

# Call the main function
asyncio.run(main())

# End time
end_time = time.time()

elapsed_time = end_time - start_time
print(f"Total time taken: {elapsed_time:.2f} seconds.")
