import os
import re
import csv

def extract_dag_info(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        
        # Extract DAG name
        dag_name_match = re.search(r'DAG\s*\(\s*"([^"]+)"', content)
        if not dag_name_match:
            return None, None
        
        dag_name = dag_name_match.group(1)
        
        # Extract ETL path
        # Look for any argument that starts with "etl/" or f"etl/"
        etl_path_match = re.search(r'(?:f?"etl/[^"]+"|"etl/[^"]+")', content)
        if not etl_path_match:
            return dag_name, None
        
        etl_path = etl_path_match.group(0)
        
        # Remove surrounding quotes and f-prefix if present
        etl_path = etl_path.strip('"').strip("'")
        if etl_path.startswith('f'):
            etl_path = etl_path[1:].strip('"').strip("'")
        
        return dag_name, etl_path

def process_directory(directory):
    dag_info_list = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                dag_name, etl_path = extract_dag_info(file_path)
                if dag_name and etl_path:
                    dag_info_list.append((dag_name, etl_path))
    
    return dag_info_list

def save_to_csv(dag_info_list, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['DAG Name', 'ETL Path'])
        for dag_name, etl_path in dag_info_list:
            csvwriter.writerow([dag_name, etl_path])

if __name__ == "__main__":
    directory = 'prod/dags'  # Update this path to your actual directory
    output_file = 'dag_info.csv'
    
    dag_info_list = process_directory(directory)
    save_to_csv(dag_info_list, output_file)
    
    print(f"Extracted {len(dag_info_list)} DAGs and saved to {output_file}")
