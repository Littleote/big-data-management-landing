import requests

# Define the URL
url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=e56bf5b3-7f94-4fa6-9b66-ae0cdd537408"

# Make the request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the response as JSON
    data = response.json()

    # Now you can work with the data
    print(data)
else:
    print(f"Request failed with status code {response.status_code}")