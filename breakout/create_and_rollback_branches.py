import os
import shutil
import subprocess
import csv

# Function to create a new branch and copy ETL content
def create_branch_and_copy_etl(dag_name, etl_path, main_branch="main"):
    try:
        # Checkout main branch
        subprocess.run(["git", "checkout", main_branch], check=True)
        
        # Create and checkout new branch
        subprocess.run(["git", "checkout", "-b", dag_name], check=True)
        
        # Create a temporary directory to hold the selected ETL content
        temp_etl_dir = "temp_etl"
        if os.path.exists(temp_etl_dir):
            shutil.rmtree(temp_etl_dir)
        os.makedirs(temp_etl_dir)
        
        # Copy only the relevant ETL content to the temporary directory
        if os.path.exists(etl_path):
            # Copy the content from etl_path to temp_etl_dir
            if os.path.isdir(etl_path):
                shutil.copytree(etl_path, os.path.join(temp_etl_dir, os.path.basename(etl_path)))
            else:
                shutil.copy2(etl_path, temp_etl_dir)
        else:
            print(f"ETL path {etl_path} does not exist. Skipping.")
            return
        
        # Remove the existing etl directory in the new branch
        if os.path.exists("etl"):
            shutil.rmtree("etl")
        
        # Move the temporary ETL content to the etl directory
        shutil.move(temp_etl_dir, "etl")
        
        # Commit changes
        subprocess.run(["git", "add", "etl"], check=True)
        subprocess.run(["git", "commit", "-m", f"Copy ETL content for {dag_name}"], check=True)
        
        # Push branch to remote
        subprocess.run(["git", "push", "origin", dag_name], check=True)
        
        print(f"Branch {dag_name} created and ETL content copied.")
    except subprocess.CalledProcessError as e:
        print(f"Error creating branch {dag_name}: {e}")

# Function to read CSV and create branches
def create_branches_from_csv(csv_file):
    with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            dag_name = row['DAG Name']
            etl_path = row['ETL Path']
            
            # Handle weird paths like etl/reconciliation/{path}/{table}
            if '{' in etl_path:
                base_path = etl_path.split('{')[0]
                if os.path.exists(base_path):
                    etl_path = base_path
                else:
                    print(f"Base path {base_path} does not exist. Skipping {dag_name}.")
                    continue
            
            create_branch_and_copy_etl(dag_name, etl_path)

# Rollback script to delete branches
def rollback_branches(csv_file):
    with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            dag_name = row['DAG Name']
            try:
                # Delete local branch
                subprocess.run(["git", "branch", "-D", dag_name], check=True)
                
                # Delete remote branch
                subprocess.run(["git", "push", "origin", "--delete", dag_name], check=True)
                
                print(f"Branch {dag_name} deleted.")
            except subprocess.CalledProcessError as e:
                print(f"Error deleting branch {dag_name}: {e}")

if __name__ == "__main__":
    csv_file = "dag_info.csv"  # Path to your CSV file
    create_branches_from_csv(csv_file)
