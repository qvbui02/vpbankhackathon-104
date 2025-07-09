from faker_generator import FakerGenerator
from schema_manager import SchemaManager
import pandas as pd
import random
import json

def main():
    '''
    # Load schema
    with open("data/customer_schema.json", "r") as f:
        schema = json.load(f)

        # Validate schema
        SchemaManager(schema)

        # Generate fake data
        faker = FakerGenerator(schema, row_counts=10000)
        data = faker.generate()

        # Export to JSON
        df = pd.DataFrame(data)
        df.to_json("data/customer.json", orient="records", lines=True, force_ascii=False)
        
        print("Data generation complete. Saved to 'customer.json'.")
    '''
    
    #Batch creation for account
    with open("data/account_schema.json", "r") as f:
        schema = json.load(f)

        SchemaManager(schema)

        #Generate up to 2 accounts per user
        u_ids = []
        for uid in range(5000):
            count = random.choice([1, 2])
            u_ids.extend([uid] * count)
        random.shuffle(u_ids)

        #Generate data using batch
        faker = FakerGenerator(schema, row_counts=len(u_ids))
        data = faker.generate_batch(u_ids)

        #Save to JSON
        df = pd.DataFrame(data)
        df.to_json("data/account.json", orient="records", lines=True, force_ascii=False)

if __name__ == "__main__":
    main()