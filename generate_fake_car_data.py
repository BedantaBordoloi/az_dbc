# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import random
import pandas as pd
from faker import Faker
import string

# Initialize Faker and create a list of car makes and models
faker = Faker()
car_makes_models = {
    'Ford': ['Focus', 'Mustang', 'Explorer'],
    'Toyota': ['Camry', 'Corolla', 'RAV4'],
    'Honda': ['Civic', 'Accord', 'CR-V'],
    'Chevrolet': ['Malibu', 'Impala', 'Equinox'],
    'BMW': ['3 Series', '5 Series', 'X5'],
    'Mercedes-Benz': ['C-Class', 'E-Class', 'GLC'],
    'Tesla': ['Model S', 'Model 3', 'Model X'],
}

# Trim levels and review texts
trim_levels = ['Base', 'Sport', 'SE', 'EX', 'Luxury']
review_texts = [
    "Great car with excellent performance.",
    "Very reliable and fuel-efficient.",
    "Comfortable ride with a lot of features.",
    "Would highly recommend to friends and family.",
    "A bit pricey but worth every penny."
]

# Function to generate a random VIN
def generate_vin():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=17))

# Function to generate a random phone number
def generate_phone_number():
    return faker.phone_number()

# Function to generate car data
def generate_car_data(num_rows):
    data = []
    for _ in range(num_rows):
        make = random.choice(list(car_makes_models.keys()))
        model = random.choice(car_makes_models[make])
        row = {
            'VIN': generate_vin(),
            'Year': random.randint(2000, 2024),
            'Make': make,
            'Model': model,
            'Trim': random.choice(trim_levels),
            'Price': round(random.uniform(5000, 100000), 2),
            'Mileage': random.randint(0, 200000),
            'SaleDate': faker.date_between(start_date='-14y', end_date='today'),
            'DealerName': faker.company().replace('\n', ' '),
            'DealerAddress': faker.address().replace('\n', ' '),
            'CustomerName': faker.name(),
            'CustomerEmail': faker.email(),
            'CustomerPhone': generate_phone_number(),
            'VehicleDescription': faker.text(max_nb_chars=200).replace('\n', ' '),
            'CustomerReview': random.choice(review_texts)
        }
        data.append(row)
    return data

# Generate 10,000 rows of car data
car_data = generate_car_data(10000)

# Convert to DataFrame
df = pd.DataFrame(car_data)

# COMMAND ----------

from datetime import datetime

# Generate a timestamped filename
filename = f"/dbfs/mnt/data/vehicle/car_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

# Save to CSV with the timestamped filename
df.to_csv(filename, index=False)
