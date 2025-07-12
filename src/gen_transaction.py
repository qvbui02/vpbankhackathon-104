import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
import json

# Đọc dữ liệu từ file account.json và customer.json
# with open("E:\\VpBank\\vpbankhackathon-104\\data\\account.json", "r") as f:
#     account_data = json.load(f)
# account_df = pd.DataFrame(account_data)

# with open("E:\\VpBank\\vpbankhackathon-104\\data\\customer.json", "r") as f:
#     customer_data = json.load(f)
# customer_df = pd.DataFrame(customer_data)
account_df = pd.read_json("E:\\VpBank\\vpbankhackathon-104\\data\\account.json", lines=True)
customer_df = pd.read_json("E:\\VpBank\\vpbankhackathon-104\\data\\customer.json", lines=True)

# Cập nhật danh sách ngân hàng và tiền tệ
# Cập nhật danh sách ngân hàng và tiền tệ
bank_currency_map = {
    'VPB': ['VND'],
    'VCB': ['VND'],
    'ACB': ['VND'],
    'TCB': ['VND'],
    'BIDV': ['VND'],
    'MB': ['VND'],
    'SAC': ['VND'],
    'HDB': ['VND'],
    'VIB': ['VND'],
    'EIB': ['VND'],
    'TPB': ['VND'],
    'VIT': ['VND'],
    'EXB': ['VND'],
    'SHB': ['VND'],
    'SCB': ['VND'],
    'CITI': ['USD', 'EUR', 'GBP'],
    'HSBC': ['USD', 'EUR', 'GBP'],
    'SC': ['USD', 'EUR', 'GBP'],
    'DB': ['USD', 'EUR', 'GBP'],
    'JPM': ['USD', 'EUR', 'GBP']
}

# Giả định mỗi account có cột bank_id (nếu chưa có thì thêm ngẫu nhiên)
if 'bank_id' not in account_df.columns:
    account_df['bank_id'] = account_df['account_id'].apply(lambda x: random.choice(list(bank_currency_map.keys())))

# Hàm tạo một cặp giao dịch
# Hàm tạo một cặp giao dịch
def generate_transaction():
    case = random.choice(["VPB_TO_OTHER", "OTHER_TO_VPB", "VPB_TO_VPB"])

    if case == "VPB_TO_OTHER":
        sender = account_df[account_df.bank_id == "VPB"].sample(1).iloc[0]
        receiver = account_df[account_df.bank_id != "VPB"].sample(1).iloc[0]
    elif case == "OTHER_TO_VPB":
        sender = account_df[account_df.bank_id != "VPB"].sample(1).iloc[0]
        receiver = account_df[account_df.bank_id == "VPB"].sample(1).iloc[0]
    else:  # VPB_TO_VPB
        sender = account_df[account_df.bank_id == "VPB"].sample(1).iloc[0]
        receiver = account_df[(account_df.bank_id == "VPB") & (account_df.account_id != sender.account_id)].sample(1).iloc[0]

    amount = random.randint(0, 50_000_000)
    request_time = datetime(2023, 1, 1) + timedelta(seconds=random.randint(0, 86400))
    complete_time = request_time + timedelta(minutes=random.randint(1, 20))
    tx_type = random.choice(["CASH", "WIRE"])
    status = random.choices([True, False], weights=[97, 3])[0]

    # Lấy tiền tệ từ bank_id
    sender_currency = random.choice(bank_currency_map[sender.bank_id])
    receiver_currency = random.choice(bank_currency_map[receiver.bank_id])
    common = {
        "sender": {
            "bank_id": sender.bank_id,
            "account_ID": sender.account_id
        },
        "receiver": {
            "bank_id": receiver.bank_id,
            "account_ID": receiver.account_id
        },
        "sender_currency": sender_currency,
        "receiver_currency": receiver_currency,
        "sender_amount": amount,
        "receiver_amount": amount,
        "request_time": request_time.strftime("%d-%m-%Y %H:%M:%S"),
        "complete_time": complete_time.strftime("%d-%m-%Y %H:%M:%S"),
        "description": f"Transfer to {receiver.account_id}",
        "status": status,
        "transaction_type": tx_type
    }

    # Nếu là VPB to VPB thì tạo 2 bản, 1 bản DEBIT và 1 bản CREDIT
    if case == "VPB_TO_VPB":
        debit_tx = {
            **common,
            "transaction_id": str(uuid.uuid4()),
            "direction": "DEBIT"
        }
        credit_tx = {
            **common,
            "transaction_id": str(uuid.uuid4()),
            "direction": "CREDIT"
        }
        return [debit_tx, credit_tx]
    elif case == "VPB_TO_OTHER":

        debit_tx = {
            **common,
            "transaction_id": str(uuid.uuid4()),
            "direction": "DEBIT"
        }
        return [debit_tx]
    else :
        credit_tx = {
            **common,
            "transaction_id": str(uuid.uuid4()),
            "direction": "CREDIT"
        }
        return [credit_tx]
# Sinh ra nhiều giao dịch
transactions = []
for _ in range(82000):  # 500 giao dịch = 1000 dòng
    transactions.extend(generate_transaction())

df = pd.DataFrame(transactions)

# Xuất ra file nếu muốn
df.to_json("transactions.json", orient="records", indent=2)
# df.to_csv("transactions.csv", index=False)

# Xem trước
print(df.head(4))
