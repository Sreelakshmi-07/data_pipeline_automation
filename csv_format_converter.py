import pandas as pd
import json
import os
from json2xml import json2xml

def csv_to_json(input_file, output_file):

    """Convert CSV data to a JSON file."""
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_file)
    
    # Convert DataFrame to JSON
    json_data = df.to_json(orient='records', indent=4)
    
    # Save JSON data to a file
    with open(output_file, 'w') as file:
        file.write(json_data)
    
    print(f"JSON file saved to {output_file}")

def csv_to_excel(input_file, output_file):

    """Convert CSV data to an Excel file."""
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_file)
    
    # Save DataFrame to an Excel file
    df.to_excel(output_file, index=False)
    print(f"Excel file saved to {output_file}")

def csv_to_xml(input_file, output_file):

    """Convert CSV data to an XML file."""
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_file)
    
    # Convert DataFrame to JSON string
    json_data = df.to_json(orient='records')
    
    # Convert JSON string to XML
    xml_data = json2xml.Json2xml(json.loads(json_data)).to_xml()
    
    # Save XML data to a file
    with open(output_file, 'w') as file:
        file.write(xml_data)
    
    print(f"XML file saved to {output_file}")

def csv_to_html(input_file, output_file):

    """Convert CSV data to an HTML file."""
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_file)
    
    # Save DataFrame to an HTML file
    df.to_html(output_file, index=False)
    print(f"HTML file saved to {output_file}")

# Example usage
input_file = input("Enter path: ") # Path to your CSV file 
base_filename = os.path.splitext(os.path.basename(input_file))[0]  # Get the base filename without extension

# Output file names based on the input file
output_json = f"{base_filename}.json"
output_excel = f"{base_filename}.xlsx"
output_xml = f"{base_filename}.xml"
output_html = f"{base_filename}.html"

#Convert CSV to various formats
csv_to_json(input_file, output_json)
csv_to_excel(input_file, output_excel)
csv_to_xml(input_file, output_xml)
csv_to_html(input_file, output_html)
