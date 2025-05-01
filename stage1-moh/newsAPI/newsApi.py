import requests
import json

url = ('https://newsapi.org/v2/everything?'
       'q=Apple&'
       'from=2024-03-20&'
       'sortBy=popularity&'
       'apiKey=5edda667c3c44888ad5bff1276d0a023')

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    
    with open('output.json', 'w') as f:
        json.dump(data, f, indent=4)  # Serialize data to JSON with indentation for readability
    
    print("Data written to output.json successfully.")
else:
    print("Error fetching data:", response.status_code)

