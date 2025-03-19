"""
Data Generator Module for metadata-driven ETL framework.
Generates sample data for testing the ETL pipeline.
"""

import random
import datetime
import uuid
from typing import List, Dict, Any
import pandas as pd
from pyspark.sql import SparkSession


class DataGenerator:
    """
    Generates sample data for testing the ETL pipeline.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize DataGenerator with SparkSession.
        
        Args:
            spark: SparkSession to use for data processing
        """
        self.spark = spark
        self.first_names = ["John", "Jane", "Robert", "Mary", "Michael", "Lisa", "David", "Sarah", "James", "Emily",
                          "William", "Olivia", "Richard", "Emma", "Joseph", "Sophia", "Thomas", "Isabella", "Charles", "Mia"]
        self.last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor",
                          "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson"]
        self.product_categories = ["Electronics", "Clothing", "Home", "Kitchen", "Toys", "Books", "Sports", "Beauty", "Grocery", "Automotive"]
        self.states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
        self.payment_methods = ["Credit Card", "Debit Card", "Cash", "PayPal", "Apple Pay", "Google Pay"]
    
    def generate_customers(self, num_customers: int = 100):
        """
        Generate sample customer data.
        
        Args:
            num_customers: Number of customers to generate
            
        Returns:
            Spark DataFrame with customer data
        """
        customers = []
        
        for i in range(num_customers):
            customer_id = f"C{str(i+1).zfill(5)}"
            first_name = random.choice(self.first_names)
            last_name = random.choice(self.last_names)
            email = f"{first_name.lower()}.{last_name.lower()}@example.com"
            phone = f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            state = random.choice(self.states)
            
            # Generate a random registration date within the last 3 years
            days_ago = random.randint(1, 365 * 3)
            registration_date = (datetime.datetime.now() - datetime.timedelta(days=days_ago)).date()
            
            customers.append({
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": phone,
                "address": f"{random.randint(100, 999)} Main St",
                "city": "Anytown",
                "state": state,
                "zip_code": f"{random.randint(10000, 99999)}",
                "registration_date": registration_date,
            })
        
        return self.spark.createDataFrame(customers)
    
    def generate_products(self, num_products: int = 200):
        """
        Generate sample product data.
        
        Args:
            num_products: Number of products to generate
            
        Returns:
            Spark DataFrame with product data
        """
        products = []
        
        for i in range(num_products):
            product_id = f"P{str(i+1).zfill(5)}"
            category = random.choice(self.product_categories)
            
            # Price between $5 and $500
            price = round(random.uniform(5, 500), 2)
            
            # Cost between 50% and 80% of price
            cost = round(price * random.uniform(0.5, 0.8), 2)
            
            # Supplier ID
            supplier_id = f"S{random.randint(1, 20):03d}"
            
            products.append({
                "product_id": product_id,
                "product_name": f"{category} Item {i+1}",
                "category": category,
                "price": price,
                "cost": cost,
                "supplier_id": supplier_id,
                "description": f"Description for {category} Item {i+1}"
            })
        
        return self.spark.createDataFrame(products)
    
    def generate_transactions(
        self, 
        num_transactions: int = 1000, 
        customer_ids: List[str] = None,
        product_ids: List[str] = None,
        start_date: str = None,
        end_date: str = None
    ):
        """
        Generate sample transaction data.
        
        Args:
            num_transactions: Number of transactions to generate
            customer_ids: List of customer IDs to use (optional)
            product_ids: List of product IDs to use (optional)
            start_date: Start date for transactions (optional, format: YYYY-MM-DD)
            end_date: End date for transactions (optional, format: YYYY-MM-DD)
            
        Returns:
            Spark DataFrame with transaction data
        """
        if customer_ids is None:
            customer_ids = [f"C{str(i+1).zfill(5)}" for i in range(100)]
            
        if product_ids is None:
            product_ids = [f"P{str(i+1).zfill(5)}" for i in range(200)]
        
        if start_date is None:
            start_date = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
            
        if end_date is None:
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        # Convert date strings to datetime objects
        start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        
        transactions = []
        
        for i in range(num_transactions):
            transaction_id = str(uuid.uuid4())
            customer_id = random.choice(customer_ids)
            product_id = random.choice(product_ids)
            
            # Random date between start_date and end_date
            days_between = (end_datetime - start_datetime).days
            random_days = random.randint(0, days_between)
            transaction_date = (start_datetime + datetime.timedelta(days=random_days)).date()
            
            # Random time on transaction date
            transaction_time = datetime.datetime.combine(
                transaction_date,
                datetime.time(
                    hour=random.randint(8, 20),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )
            )
            
            # Quantity between 1 and 5
            quantity = random.randint(1, 5)
            
            # Amount between $10 and $200 per item
            amount_per_item = round(random.uniform(10, 200), 2)
            amount = round(amount_per_item * quantity, 2)
            
            # Store ID
            store_id = f"ST{random.randint(1, 10):02d}"
            
            # Payment method
            payment_method = random.choice(self.payment_methods)
            
            transactions.append({
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "transaction_date": transaction_date,
                "transaction_time": transaction_time,
                "amount": amount,
                "quantity": quantity,
                "store_id": store_id,
                "payment_method": payment_method
            })
        
        return self.spark.createDataFrame(transactions)
    
    def generate_and_save_data(self, output_path: str = "/data/raw") -> None:
        """
        Generate and save sample data to the specified path.
        
        Args:
            output_path: Path to save the generated data
        """
        # Generate customer data
        customer_spark_df = self.generate_customers(num_customers=500)
        customer_spark_df.write.mode("overwrite").csv(f"{output_path}/customers", header=True)
        
        # Generate product data
        product_spark_df = self.generate_products(num_products=1000)
        product_spark_df.write.mode("overwrite").json(f"{output_path}/products")
        
        # Generate transaction data for the last 90 days
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=180)).strftime("%Y-%m-%d")
        
        # Get customer and product IDs
        customer_ids = [row.customer_id for row in customer_spark_df.select("customer_id").collect()]
        product_ids = [row.product_id for row in product_spark_df.select("product_id").collect()]
        
        transaction_spark_df = self.generate_transactions(
            num_transactions=5000,
            customer_ids=customer_ids,
            product_ids=product_ids,
            start_date=start_date,
            end_date=end_date
        )
        
        transaction_spark_df.write.mode("overwrite").parquet(f"{output_path}/transactions") 