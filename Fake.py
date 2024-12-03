# data_generator.py

import json
import time
from faker import Faker

fake = Faker()

def generate_order():
    order = {
        "id": fake.uuid4(),
        "name": fake.name(),
        "quantity": fake.random_int(min=1, max=10)
    }
    return order

if __name__ == "__main__":
    while True:
        order = generate_order()
        print(json.dumps(order))
        time.sleep(1)  # Adjust the interval as required
