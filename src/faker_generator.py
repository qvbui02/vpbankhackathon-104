__file__ = "faker_generator.py"
__author__ = "Benji Bui"
__date__ = "2025/07"
__email__ = "bui.vquang02@gmail.com"
__version__ = "0.0.1"

import pandas as pd
import random
from faker import Faker

class FakerGenerator:
    def __init__(self, schema, row_counts):
        self.faker = Faker('vi_VN')
        self.schema = schema
        self.row_counts = row_counts
        self.generated_cccd = set() # lookup time O(1)
        
    def generate(self):
        records = []
        
        for uid in range(0, self.row_counts):
            record = {}
            
            for col, props in self.schema['columns'].items():
                sdtype = props['sdtype']
                if col == 'UID':
                    value = uid
                elif col == 'cccd':
                    value = self.generate_cccd()
                else:
                    value = self.generate_values(sdtype, col)
                
                record[col] = value
            
            records.append(record)
            
        return records
            
    def generate_cccd(self):
        while True:
            cccd = "0" + str(random.randint(10**10, 10**11-1)) # 10,000,000,000 - 99,999,999,999
            if cccd not in self.generated_cccd:
                self.generated_cccd.add(cccd)
                return cccd
    
    def generate_values(self, sdtype, col):
        if col == 'first_name':
            return self.faker.first_name_unisex()
        elif col == 'middle_name':
            return self.faker.middle_name()
        elif col == 'last_name':
            return self.faker.last_name()
        elif col == 'address':
            return self.faker.street_name()
        elif col == 'district':
            return self.faker.address()
        elif col == 'city':
            return self.faker.administrative_unit()
        elif col == 'country':
            return "Việt Nam"
        elif col == 'number':
            return self.faker.numerify("09# ### ####")
        elif col == 'dob':
            return self.faker.date()
        elif col == 'gender':
            return random.choice(["M", "F"])
        elif col == 'email':
            return self.faker.email()
        else:
            return None
        
    
        