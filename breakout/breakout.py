import csv
import os
import shutil
from git import Repo

# Path to your CSV file
csv_file_path = 'path_to_your_csv_file.csv'

# Path to your local repository
repo_path = 'path_to_your_local_repo'

# Initialize the repository
repo = Repo(repo_path)

# Read the CSV file
with open(csv_file_path, mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    
    for row in csv_reader:
        dag_id = row['dag_id']
        python_file_path = row['python_file_path']
        owner = row['owner']
        
        # Create branch name
        branch_name = f"DEPG-745 {dag_id}"
        
        # Checkout main branch and pull latest changes
        repo.git.checkout('main')
        repo.git.pull()
        
        # Create and checkout new branch
        repo.git.checkout('-b', branch_name)
        
        # Remove all files except the specified Python file and etl directory
        for item in os.listdir(repo_path):
            item_path = os.path.join(repo_path, item)
            if item != 'etl' and item != os.path.basename(python_file_path):
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
        
        # Move the specified Python file to the root directory if it's not already there
        if os.path.dirname(python_file_path) != repo_path:
            shutil.move(os.path.join(repo_path, python_file_path), repo_path)
        
        # Commit the changes
        repo.git.add('--all')
        repo.git.commit('-m', f"Add {dag_id} related files")
        
        # Push the branch to remote
        repo.git.push('origin', branch_name)
        
        print(f"Branch {branch_name} created and pushed successfully.")

print("All branches created and pushed successfully.")
