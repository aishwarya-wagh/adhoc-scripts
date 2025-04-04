import pandas as pd

# Read the input CSV file
input_file = 'input.csv'  # Replace with your input file path
output_file = 'output.csv'  # Replace with your desired output file path

try:
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Filter rows where TABLE_TYPE is "VIEW"
    filtered_df = df[df['TABLE_TYPE'] == 'VIEW']
    
    # Save the filtered data to a new CSV file
    filtered_df.to_csv(output_file, index=False)
    
    print(f"Successfully saved {len(filtered_df)} rows to {output_file}")
    
except FileNotFoundError:
    print(f"Error: Input file '{input_file}' not found")
except KeyError:
    print("Error: 'TABLE_TYPE' column not found in the input file")
except Exception as e:
    print(f"An error occurred: {str(e)}")
