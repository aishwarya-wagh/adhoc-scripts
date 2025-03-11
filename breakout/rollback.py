import os
from git import Repo

# Path to your local repository
repo_path = 'path_to_your_local_repo'

# Initialize the repository
repo = Repo(repo_path)

# Step 1: Delete created branches
# Fetch all branches from remote
repo.git.fetch('--all')

# Get list of all branches
branches = repo.git.branch('-a').split()

# Filter branches that match the naming pattern 'DEPG-745 <dag_id>'
branches_to_delete = [branch for branch in branches if branch.startswith('DEPG-745')]

# Delete branches
for branch in branches_to_delete:
    repo.git.push('origin', '--delete', branch)
    print(f"Deleted branch {branch} from remote.")

# Step 2: Reset local repository to main branch
repo.git.checkout('main')
repo.git.reset('--hard', 'origin/main')
print("Reset local repository to main branch.")

# Step 3: Clean up untracked files
repo.git.clean('-fd')
print("Cleaned up untracked files.")

print("Rollback completed successfully.")
