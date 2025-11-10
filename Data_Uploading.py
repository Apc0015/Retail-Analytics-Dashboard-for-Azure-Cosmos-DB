"""
Retail Analytics Data Upload
Ayush Chhoker - U00363568
"""

import pymongo
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, PyMongoError
import random
from datetime import datetime, timedelta
import time
import os
import sys

# Database connection
# Set COSMOS_CONNECTION_STRING environment variable before running
COSMOS_CONNECTION_STRING = os.environ.get('COSMOS_CONNECTION_STRING')
if not COSMOS_CONNECTION_STRING:
    print("ERROR: Please set COSMOS_CONNECTION_STRING environment variable")
    print("See README.md for setup instructions")
    sys.exit(1)

DATABASE_NAME = "retail_analytics"
COLLECTIONS = {
    'products': 'products',
    'customers': 'customers',
    'orders': 'orders',
    'reviews': 'reviews'
}

RESET_DATABASE = False
FORCE_UPLOAD = False
MIN_DOCS_PER_COLLECTION = 50
BATCH_SIZE = 5
BATCH_DELAY = 2

def generate_products(n=100):
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Beauty', 'Food']
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE', 'Generic']

    products = []
    for i in range(1, n + 1):
        category = random.choice(categories)
        product_id = f'P{i:04d}'
        product = {
            'id': product_id,
            'product_id': product_id,
            'name': f'{category} Product {i}',
            'category': category,
            'brand': random.choice(brands),
            'price': round(random.uniform(10, 500), 2),
            'cost': round(random.uniform(5, 250), 2),
            'stock_quantity': random.randint(0, 500),
            'rating': round(random.uniform(3.0, 5.0), 1),
            'num_reviews': random.randint(0, 200),
            'created_date': (datetime.now() - timedelta(days=random.randint(30, 365))).isoformat()
        }
        products.append(product)

    return products

def generate_customers(n=200):
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
              'San Antonio', 'San Diego', 'Dallas', 'San Jose']
    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA']

    customers = []
    for i in range(1, n + 1):
        city_idx = random.randint(0, len(cities) - 1)
        customer_id = f'C{i:05d}'
        customer = {
            'id': customer_id,
            'customer_id': customer_id,
            'name': f'Customer {i}',
            'email': f'customer{i}@email.com',
            'city': cities[city_idx],
            'state': states[city_idx],
            'join_date': (datetime.now() - timedelta(days=random.randint(1, 730))).isoformat(),
            'loyalty_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            'total_spent': 0,
            'order_count': 0
        }
        customers.append(customer)

    return customers

def generate_orders(customers, products, n=500):
    orders = []
    for i in range(1, n + 1):
        customer = random.choice(customers)
        order_date = datetime.now() - timedelta(days=random.randint(0, 180))
        
        num_items = random.randint(1, 5)
        items = []
        total_amount = 0

        for _ in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 3)
            item_total = product['price'] * quantity
            total_amount += item_total

            items.append({
                'product_id': product['product_id'],
                'product_name': product['name'],
                'category': product['category'],
                'quantity': quantity,
                'unit_price': product['price'],
                'item_total': round(item_total, 2)
            })

        order = {
            'order_id': f'O{i:06d}',
            'id': f'O{i:06d}',  # Add id field for Cosmos DB
            'customer_id': customer['customer_id'],
            'customer_name': customer['name'],
            'customer_city': customer['city'],
            'customer_state': customer['state'],
            'order_date': order_date.isoformat(),
            'items': items,
            'total_amount': round(total_amount, 2),
            'shipping_cost': round(random.uniform(5, 15), 2),
            'tax': round(total_amount * 0.08, 2),
            'status': random.choice(['Completed', 'Completed', 'Completed', 'Pending', 'Shipped']),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash'])
        }
        orders.append(order)

    return orders

def generate_reviews(orders, n=300):
    reviews = []
    for i in range(1, n + 1):
        order = random.choice(orders)
        if order['items']:
            item = random.choice(order['items'])
            rating = random.randint(1, 5)
            review_texts = {
                1: "Very disappointed with this purchase. Would not recommend.",
                2: "Below expectations. Several issues encountered.",
                3: "Average product. Does the job but nothing special.",
                4: "Good product. Happy with the purchase overall.",
                5: "Excellent quality! Exceeded my expectations. Highly recommend!"
            }

            review = {
                'review_id': f'R{i:05d}',
                'id': f'R{i:05d}',
                'order_id': order['order_id'],
                'product_id': item['product_id'],
                'product_name': item['product_name'],
                'customer_id': order['customer_id'],
                'customer_name': order['customer_name'],
                'rating': rating,
                'review_text': review_texts.get(rating, "Good product"),
                'review_date': (datetime.fromisoformat(order['order_date']) + timedelta(days=random.randint(1, 30))).isoformat(),
                'helpful_votes': random.randint(0, 50)
            }
            reviews.append(review)

    return reviews

def upload_to_cosmos_db():
    print("Connecting to Azure Cosmos DB...")
    try:
        client = MongoClient(COSMOS_CONNECTION_STRING, serverSelectionTimeoutMS=10000)
        client.server_info()
        print("✓ Successfully connected to Azure Cosmos DB")

        if RESET_DATABASE:
            print(f"\n⚠ RESET_DATABASE is True - dropping database '{DATABASE_NAME}'...")
            try:
                client.drop_database(DATABASE_NAME)
                print(f"✓ Database '{DATABASE_NAME}' dropped")
                time.sleep(2)
            except Exception as e:
                print(f"Note: Could not drop database (may not exist): {e}")

        db = client[DATABASE_NAME]
        print(f"✓ Using database: {DATABASE_NAME}")
        print("\n" + "="*50)
        print("GENERATING DATA")
        print("="*50)
        
        n_products = max(MIN_DOCS_PER_COLLECTION, 25)
        n_customers = max(MIN_DOCS_PER_COLLECTION, 25)
        n_orders = max(MIN_DOCS_PER_COLLECTION, 30)
        n_reviews = max(MIN_DOCS_PER_COLLECTION, 25)
        
        products = generate_products(n_products)
        customers = generate_customers(n_customers)
        orders = generate_orders(customers, products, n_orders)
        reviews = generate_reviews(orders, n_reviews)

        print(f"✓ Generated {len(products)} products")
        print(f"✓ Generated {len(customers)} customers")
        print(f"✓ Generated {len(orders)} orders")
        print(f"✓ Generated {len(reviews)} reviews")

        print("\nChecking existing collections...")
        existing_collections = db.list_collection_names()
        for collection_name in COLLECTIONS.values():
            if collection_name in existing_collections:
                count = db[collection_name].count_documents({})
                print(f"  {collection_name}: {count} existing documents (will skip if populated)")
            else:
                print(f"  {collection_name}: new collection")

        print("\nUploading data to Cosmos DB in batches...")

        def insert_in_batches(collection, data, batch_size=10):
            total = len(data)
            inserted = 0
            max_retries = 5

            for i in range(0, total, batch_size):
                batch = data[i:i + batch_size]
                requests = [InsertOne(doc) for doc in batch]
                attempt = 0
                
                while attempt <= max_retries:
                    try:
                        result = collection.bulk_write(requests, ordered=False)
                        batch_inserted = result.inserted_count if hasattr(result, 'inserted_count') else len(batch)
                        inserted += batch_inserted
                        print(f"  Inserted {inserted}/{total} documents (batch {i//batch_size + 1})")
                        time.sleep(BATCH_DELAY)
                        break
                    except BulkWriteError as bwe:
                        details = bwe.details if hasattr(bwe, 'details') else {}
                        n_success = details.get('nInserted', 0) if details else 0
                        inserted += n_success
                        errors = details.get('writeErrors', []) if details else []
                        print(f"  BulkWriteError: inserted {n_success}/{len(batch)}; errors: {len(errors)}")
                        if errors:
                            print(f"    Sample error: {errors[0].get('errmsg', 'Unknown')}")
                        time.sleep(BATCH_DELAY * 2)
                        break
                    except PyMongoError as e:
                        msg = str(e)
                        if '16500' in msg or '429' in msg or 'RequestRateTooLarge' in msg or 'TooManyRequests' in msg or 'RetryAfterMs' in msg:
                            backoff = (2 ** attempt) * BATCH_DELAY + (random.random() * 0.5)
                            print(f"  Rate limited, retrying in {backoff:.1f}s (attempt {attempt+1}/{max_retries})")
                            time.sleep(backoff)
                            attempt += 1
                            if attempt > max_retries:
                                print(f"  Max retries exceeded for batch, skipping...")
                                break
                            continue
                        else:
                            print(f"  PyMongoError: {msg[:200]}")
                            for doc in batch:
                                try:
                                    collection.insert_one(doc)
                                    inserted += 1
                                    time.sleep(0.5)
                                except Exception as e2:
                                    print(f"    Failed to insert document: {str(e2)[:100]}")
                            break

            return inserted

        def clear_collection_safely(collection, batch_size=20):
            deleted = 0
            while True:
                docs = list(collection.find({}, {'_id': 1}).limit(batch_size))
                if not docs:
                    break
                ids = [doc['_id'] for doc in docs]
                try:
                    result = collection.delete_many({'_id': {'$in': ids}})
                    deleted += result.deleted_count
                    time.sleep(1.5)
                except PyMongoError as e:
                    print(f"  Delete error: {str(e)[:100]}, retrying...")
                    time.sleep(3)
            return deleted

        print(f"\nUploading products...")
        products_count = db[COLLECTIONS['products']].count_documents({})
        if FORCE_UPLOAD or products_count < MIN_DOCS_PER_COLLECTION:
            if products_count > 0 and FORCE_UPLOAD:
                print(f"  Clearing existing {products_count} documents...")
                deleted = clear_collection_safely(db[COLLECTIONS['products']])
                print(f"  Deleted {deleted} old documents")
            inserted = insert_in_batches(db[COLLECTIONS['products']], products)
            print(f"✓ Uploaded {inserted} products to {COLLECTIONS['products']} collection")
        else:
            print(f"✓ Skipping products - {products_count} documents already exist")

        print(f"\nUploading customers...")
        customers_count = db[COLLECTIONS['customers']].count_documents({})
        if FORCE_UPLOAD or customers_count < MIN_DOCS_PER_COLLECTION:
            if customers_count > 0 and FORCE_UPLOAD:
                print(f"  Clearing existing {customers_count} documents...")
                deleted = clear_collection_safely(db[COLLECTIONS['customers']])
                print(f"  Deleted {deleted} old documents")
            inserted = insert_in_batches(db[COLLECTIONS['customers']], customers)
            print(f"✓ Uploaded {inserted} customers to {COLLECTIONS['customers']} collection")
        else:
            print(f"✓ Skipping customers - {customers_count} documents already exist")

        print(f"\nUploading orders...")
        orders_count = db[COLLECTIONS['orders']].count_documents({})
        if FORCE_UPLOAD or orders_count < MIN_DOCS_PER_COLLECTION:
            if orders_count > 0 and FORCE_UPLOAD:
                print(f"  Clearing existing {orders_count} documents...")
                deleted = clear_collection_safely(db[COLLECTIONS['orders']])
                print(f"  Deleted {deleted} old documents")
            inserted = insert_in_batches(db[COLLECTIONS['orders']], orders)
            print(f"✓ Uploaded {inserted} orders to {COLLECTIONS['orders']} collection")
        else:
            print(f"✓ Skipping orders - {orders_count} documents already exist")

        print(f"\nUploading reviews...")
        reviews_count = db[COLLECTIONS['reviews']].count_documents({})
        if FORCE_UPLOAD or reviews_count < MIN_DOCS_PER_COLLECTION:
            if reviews_count > 0 and FORCE_UPLOAD:
                print(f"  Clearing existing {reviews_count} documents...")
                deleted = clear_collection_safely(db[COLLECTIONS['reviews']])
                print(f"  Deleted {deleted} old documents")
            inserted = insert_in_batches(db[COLLECTIONS['reviews']], reviews)
            print(f"✓ Uploaded {inserted} reviews to {COLLECTIONS['reviews']} collection")
        else:
            print(f"✓ Skipping reviews - {reviews_count} documents already exist")

        print("\n" + "="*50)
        print("VERIFICATION & SUMMARY")
        print("="*50)
        
        total_expected = len(products) + len(customers) + len(orders) + len(reviews)
        total_uploaded = 0
        all_success = True
        
        print(f"\nDatabase: {DATABASE_NAME}")
        print(f"Collections: {len(COLLECTIONS)}\n")
        
        for name, collection in COLLECTIONS.items():
            count = db[collection].count_documents({})
            total_uploaded += count
            expected = len(locals()[name])
            status = "✓" if count >= MIN_DOCS_PER_COLLECTION else "✗"
            print(f"  {status} {collection:12s}: {count:4d} documents (expected: {expected})")
            if count < MIN_DOCS_PER_COLLECTION:
                all_success = False
        
        print(f"\nTotal uploaded: {total_uploaded}/{total_expected} documents")
        
        if all_success:
            print("\n✓ SUCCESS: All collections meet minimum document requirements")
        else:
            print(f"\n⚠ WARNING: Some collections have fewer than {MIN_DOCS_PER_COLLECTION} documents")
        
        # Sample verification - show a few documents
        print("\n" + "="*50)
        print("SAMPLE DOCUMENTS")
        print("="*50)
        for coll_name in ['products', 'orders', 'reviews']:
            sample = db[COLLECTIONS[coll_name]].find_one()
            if sample:
                print(f"\nSample {coll_name} document:")
                print(f"  id: {sample.get('id', 'N/A')}")
                if coll_name == 'orders':
                    print(f"  customer_id: {sample.get('customer_id')}")
                    print(f"  items: {len(sample.get('items', []))} products")
                elif coll_name == 'reviews':
                    print(f"  product_id: {sample.get('product_id')}")
                    print(f"  customer_id: {sample.get('customer_id')}")
                    print(f"  order_id: {sample.get('order_id')}")
                    print(f"  rating: {sample.get('rating')}")

        client.close()
        return all_success

    except Exception as e:
        print(f"\n✗ FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("="*50)
    print("E-COMMERCE DATA UPLOAD TO AZURE COSMOS DB")
    print("="*50)
    print(f"Reset Database: {RESET_DATABASE}")
    print(f"Minimum Docs Per Collection: {MIN_DOCS_PER_COLLECTION}")
    print("="*50)
    
    success = upload_to_cosmos_db()
    sys.exit(0 if success else 1)
