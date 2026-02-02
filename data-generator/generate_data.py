import json
import random
import uuid
from datetime import datetime, timedelta

pages = ['/home','/products','/cart','/checkout']
event = ['view','click','purchase']

def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1,100),
        "page": random.choice(pages),
        "event_type": random.choice(event),
        "timestamp": (datetime.now() - timedelta(seconds=random.randint(1,10000))).isoformat()
    }
with open("clickstream.json", "w") as f:
    for _ in range(100):
        f.write(json.dumps(generate_event()) + '\n')
