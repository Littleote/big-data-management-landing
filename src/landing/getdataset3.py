from bs4 import BeautifulSoup
import requests
import csv
import os
import json

# Define the base URL
base_url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id="

# Define the webpage URL
webpage_url = "https://opendata-ajuntament.barcelona.cat/data/en/dataset/pad_cdo_b_barri-des"

# Send a GET request to the webpage
response = requests.get(webpage_url)

# Parse the response content with BeautifulSoup
soup = BeautifulSoup(response.content, 'html.parser')

# Find the section element with id "dataset-resources"
section_element = soup.find('section', id='dataset-resources')

# Find all div elements with class "accordion-group" within the section
div_elements = section_element.find_all('div', class_='accordion-group')

# Initialize an empty list to store the URLs
urls = []

# For each div element
for div in div_elements:
    # Find the first li element with class "resource-item"
    li_element = div.find('li', class_='resource-item')

    # Get the "data-id" attribute
    data_id = li_element.get('data-id')

    # Append the data_id to the base URL and add it to the list
    urls.append(base_url + data_id)

# Now you have a list of URLs in the "urls" variable

# Start the year at 2022
year = 2022

# For each URL in the list of URLs
for url in urls:
    # Send a GET request to the URL
    apiresponse = requests.get(url)

    # Check if the request was successful
    if apiresponse.status_code == 200:
        # Parse the response as JSON
        data = apiresponse.json()

        # Check if the response contains data
        if data and data['success'] and 'result' in data and isinstance(data['result'], dict):
            result_data = data['result']

            # Define the directory to save the CSV files
            directory = '../../data/opendatabcn-domicile-changes'

            # Open a new CSV file in write mode
            with open(os.path.join(directory, f'data_{year}.csv'), 'w', newline='') as file:
                # Create a CSV writer
                writer = csv.writer(file)

                # Write the JSON data to the CSV file
                writer.writerow([field['id'] for field in result_data['fields']])
                writer.writerows(
                    [record[field['id']] for field in result_data['fields']] for record in result_data['records'])
        else:
            print(f"Response from {url} does not contain valid data")
    else:
        print(f"Request to {url} failed with status code {apiresponse.status_code}")

    # Decrement the year
    year -= 1