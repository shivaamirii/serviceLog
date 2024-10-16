import random
import pandas as pd
from persiantools.jdatetime import JalaliDate
import clickhouse_connect

# Connect to ClickHouse server
client = clickhouse_connect.get_client(host='localhost', port=8123)

table_name = "service_call" # it can change 

# Create the table if it doesn't exist
# this method is used to execute SQL commands on ClickHouse. 
client.command(f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    jalali_year Int32,
    jalali_month Int32,
    jalali_day Int32,
    service_name String,
    provider_name String,
    consumer_name String,
    response_code Int32
) ENGINE = MergeTree()
ORDER BY (jalali_year, jalali_month, jalali_day)
''')

# Define the parameters
jalali_years = [1402, 1403]
jalali_month_days = {
    1: 31, 2: 31, 3: 31, 4: 31, 5: 31, 6: 31,  # 31 days months
    7: 30, 8: 30, 9: 30, 10: 30, 11: 30,       # 30 days months
    12: 29                                       # 29 days in month 12
}
service_names = [f"service_{number}" for number in range(1, 51)]  # 50 random services
providers = ['sabtahval', 'naji', 'vezaratkeshvar', 'eadlir', 'sazmanbourse', 
             'taxgovir', 'eghtesad', 'tavnir', 'gas', 'abofazelab']  # 10 providers
consumers = [f"consumer_org_{number}" for number in range(1, 26)]  # 25 random consumer organizations

# Assign each service to a single provider
service_provider_mapping = {service: random.choice(providers) for service in service_names}

response_code_distribution = {
    '200': 60,  # 60% chance
    '300': 5,   # 5% chance
    '400': 20,  # 20% chance
    '500': 15   # 15% chance
}
response_codes = [200, 300, 400, 500]

# Adjusted function to select only specific codes based on probability distribution
def get_response_code():
    rand = random.randint(1, 100)
    if rand <= 60:
        return 200
    elif rand <= 65:
        return 300
    elif rand <= 85:
        return 400
    else:
        return 500


# Function to generate daily service calls
def generate_daily_calls(jalali_year, jalali_month, jalali_day):
    num_calls = random.randint(400, 500)  # Ensure between 400-500 calls per day
    rows = []
    
    for count in range(num_calls):
        service_name = random.choice(service_names)
        provider_name = service_provider_mapping[service_name]  # Get the specific provider for this serviceprovider_name = random.choice(providers)
        consumer_name = random.choice(consumers)
        response_code = get_response_code()

        rows.append((jalali_year, jalali_month, jalali_day, service_name, provider_name, consumer_name, response_code))
    
    return rows

# Generate data for all years, months, and days
data = []
for year in jalali_years:
    for month, days_in_month in jalali_month_days.items():
        for day in range(1, days_in_month + 1):
            data.extend(generate_daily_calls(year, month, day))

# Insert data into ClickHouse
client.insert(table_name, data, column_names=['jalali_year', 'jalali_month', 'jalali_day', 'service_name', 'provider_name', 'consumer_name', 'response_code'])

print("Data inserted successfully into ClickHouse!")
