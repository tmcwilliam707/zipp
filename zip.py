import os
import zipfile

# Define source and target directories
source_dir = "/Users/taylormcwilliam/Downloads/698426317559824384/files"
target_dir = os.path.join(source_dir, "unzip_files")

# Create target directory if it doesn't exist
os.makedirs(target_dir, exist_ok=True)

# Iterate through all files in the source directory
for file_name in os.listdir(source_dir):
    if file_name.endswith(".zip"):
        zip_path = os.path.join(source_dir, file_name)
        try:
            # Unzip the file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(target_dir)
            print(f"Extracted: {file_name}")
        except zipfile.BadZipFile:
            print(f"Error: {file_name} is not a valid zip file.")