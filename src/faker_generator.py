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
        self.generated_cccd = set() #lookup time O(1)
        self.available_a_ids = random.sample(range(self.row_counts * 2), self.row_counts)
        self.vpbank_account_header = ["10", "11", "12", "13", "14", "15", "16", "17", "18", "21", "69", "79", "82", "87"]
        self.available_account_ids = self.generate_unique_account_ids()
        self.available_u_ids = self.generate_unique_u_ids()
        
        
    def generate_batch(self, u_id_list):
        records = []

        for u_id in u_id_list:
            record = {}
            for col, props in self.schema['columns'].items():
                sdtype = props['sdtype']
                if col == 'u_id':
                    value = u_id
                elif col == 'a_id':
                    value = self.generate_a_id()
                elif col == 'cccd':
                    value = self.generate_cccd()
                elif col == 'account_id':
                    value = self.generate_account_id()
                else:
                    value = self.generate_values(sdtype, col)
                record[col] = value
            records.append(record)

        return records

        
    def generate(self):
        records = []
        
        for _ in range(0, self.row_counts):
            record = {}
            
            for col, props in self.schema['columns'].items():
                sdtype = props['sdtype']
                if col == 'u_id':
                    value = self.generate_u_id()
                elif col == 'a_id':
                    value = self.generate_a_id()
                elif col == 'cccd':
                    value = self.generate_cccd()
                elif col == 'account_id':
                    value = self.generate_account_id()
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
    
    def generate_unique_u_ids(self):
        return list(range(self.row_counts))
    
    def generate_u_id(self):
        return self.available_u_ids.pop(0) # customer table
        # account table
        ids = list(range(self.row_counts))
        random.shuffle(ids)
        return ids

    def generate_a_id(self):
        return self.available_a_ids.pop()
    
    def generate_unique_account_ids(self):
        generated = set()
        account_ids = []
        while len(account_ids) < self.row_counts:
            prefix = random.choice(self.vpbank_account_header)
            suffix = str(random.randint(100000, 999999))  # 6 digits
            acct_id = prefix + suffix  # 8-digit total
            if acct_id not in generated:
                generated.add(acct_id)
                account_ids.append(acct_id)
        random.shuffle(account_ids)
        return account_ids
            
    def generate_account_id(self):
        return self.available_account_ids.pop()
    
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
        elif col == 'dob' or col == 'created_date':
            return self.faker.date()
        elif col == 'gender':
            return random.choice(["M", "F"])
        elif col == 'email':
            return self.faker.email()
        elif col == 'account_status':
            return "active"
        else:
            return None
        
    
        