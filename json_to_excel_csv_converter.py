import json
import pandas as pd

def flatten_dict(d, parent_key='', sep='_'):

    """Recursively flatten a nested dictionary."""

    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            # Recursively flatten nested dictionaries
            items.extend(flatten_dict(v, new_key, sep=sep).items())
            print(items)
        elif isinstance(v, list):
            # Handle lists by joining the items into a string
            items.append((new_key, ', '.join(map(str, v))))
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_json(json_data):

    """
    Given a JSON object, return a flattened version suitable for conversion to other formats.
    """

    flattened_data = []
    for item in json_data.values():
        flattened_data.append(flatten_dict(item))
    return flattened_data

def json_to_excel(input_file, output_file):

    """Convert the flattened JSON data from a file into an Excel file."""

    # Read the JSON file
    with open(input_file, 'r') as file:
        json_data = json.load(file)

    # Flatten the JSON
    flattened_json = flatten_json(json_data)

    # Convert to pandas DataFrame
    df = pd.DataFrame(flattened_json)

    # Save to Excel
    df.to_excel(output_file, index=False)
    print(f"Excel file saved to {output_file}")

def json_to_csv(input_file, output_file):

    """Convert the flattened JSON data from a file into a CSV file."""

    # Read the JSON file
    with open(input_file, 'r') as file:
        json_data = json.load(file)

    # Flatten the JSON
    flattened_json = flatten_json(json_data)

    # Convert to pandas DataFrame
    df = pd.DataFrame(flattened_json)

    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"CSV file saved to {output_file}")


# Example usage
input_file = input("Enter path: ") # Path to your JSON file 
output_path = f"{input_file.rsplit('/', 1)[-1]}"
output_excel = output_path.replace(".json", ".xlsx") # Path to save the Excel file
output_csv = output_path.replace(".json", ".csv")  # Path to save the CSV file

# Convert JSON to Excel
json_to_excel(input_file, output_excel)
# Convert JSON to CSV
json_to_csv(input_file, output_csv)



