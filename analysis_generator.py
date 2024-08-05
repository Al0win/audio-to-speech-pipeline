import subprocess

# Define the local file path to your repository
local_repo_path = "/home/alowin/Documents/Bhashini/audio-to-speech-pipeline"  # Update this to your local repo path

# Get the list of modified files
try:
    modified_files = subprocess.check_output(
        ["git", "diff", "--name-only", "origin/master"],
        cwd=local_repo_path
    ).decode("utf-8").splitlines()
    
    # Print the list of modified files
    print("Modified files:")

    count = 0
    for file in modified_files:
        print(file)
        count += 1
    print(count , "Different Files")

except subprocess.CalledProcessError as e:
    print(f"An error occurred while running git diff: {e}")
