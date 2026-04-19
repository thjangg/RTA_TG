from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

# YOUR CODE
# For each message:
#   1. Increment store_counts[store]
#   2. Add amount to total_amount[store]
#   3. Every 10 messages, print a summary table:
#      Store | Count | Total Amount | Avg Amount
