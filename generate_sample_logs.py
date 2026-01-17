import csv
import random
from datetime import datetime, timedelta

# Generate sample OTT logs
shows = ['S201', 'S202', 'S203', 'S204', 'S205']
devices = ['Mobile', 'TV', 'Laptop', 'Tablet']
users = [f'U{i:04d}' for i in range(1, 101)]

output_file = 'view_log_2025-12-20.csv'

with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f) 
    writer.writerow(['user_id', 'show_id', 'duration_minutes', 'device_type', 'view_date'])
    
    for _ in range(10000):  # 10,000 records
        user = random.choice(users)
        show = random.choice(shows)
        duration = random.randint(5, 180)  # 5-180 minutes
        device = random.choice(devices)
        date = '2025-12-20'
        
        writer.writerow([user, show, duration, device, date])

print(f"Generated {output_file}")
