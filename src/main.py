from faker_generator import FakerGenerator
from schema_manager import SchemaManager
import pandas as pd
import json

def main():
    # Load schema
    with open("data/customer_schema.json", "r") as f:
        schema = json.load(f)

    # Validate schema
    SchemaManager(schema)

    # Generate fake data
    faker = FakerGenerator(schema, row_counts=100)
    data = faker.generate()

    # Export to JSON
    df = pd.DataFrame(data)
    df.to_json("data/customer.json", orient="records", indent=2, force_ascii=False)
    print("Data generation complete. Saved to 'customer.json'.")

if __name__ == "__main__":
    main()