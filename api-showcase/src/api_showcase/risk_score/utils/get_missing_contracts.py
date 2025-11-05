import os
from pathlib import Path
import shutil

def find_missing_files(input_folder_path, output_folder_paths, output_txt_path=None):
    """
    Find files that exist in input folder but not in any of the output folders.
    
    Args:
        input_folder_path: Path to the input folder
        output_folder_paths: List of paths to output folders
        output_txt_path: Optional path to save missing filenames to a text file
    
    Returns:
        List of filenames that exist in input folder but not in any output folder
    """
    # Get all filenames from input folder
    input_files = set()
    if os.path.exists(input_folder_path):
        input_files = {f for f in os.listdir(input_folder_path) 
                      if os.path.isfile(os.path.join(input_folder_path, f))}
    else:
        print(f"Warning: Input folder '{input_folder_path}' does not exist")
        return []
    
    # Get all filenames from all output folders
    output_files = set()
    for output_path in output_folder_paths:
        if os.path.exists(output_path):
            files = {f for f in os.listdir(output_path) 
                    if os.path.isfile(os.path.join(output_path, f))}
            output_files.update(files)
        else:
            print(f"Warning: Output folder '{output_path}' does not exist")
    
    # Find files in input that are not in any output folder
    missing_files = sorted(list(input_files - output_files))
    
    # Print results
    print(f"Files in input folder but not in any output folder:")
    print(missing_files)
    
    # Write to text file if path provided
    if output_txt_path:
        try:
            with open(output_txt_path, 'w') as f:
                for filename in missing_files:
                    f.write(f"{filename}\n")
            print(f"\nMissing files written to: {output_txt_path}")
        except Exception as e:
            print(f"Error writing to file: {e}")
    
    return missing_files

# Example usage
if __name__ == "__main__":
    # Example paths - replace with your actual paths
    input_folder = "/Users/daniellanghann/src/api-showcase/api-showcase/src/api_showcase/rest_importer/test_documents/ALL"
    output_folders = [
        "/Users/daniellanghann/src/api-showcase/api-showcase/src/api_showcase/rest_importer/test_documents/1_IMD",
        "/Users/daniellanghann/src/api-showcase/api-showcase/src/api_showcase/rest_importer/test_documents/2_IDD",
        "/Users/daniellanghann/src/api-showcase/api-showcase/src/api_showcase/rest_importer/test_documents/3_ESG"
    ]
    
       # Specify where to save the missing files list
    txt_file_path = "missing_files.txt"
    
    missing = find_missing_files(input_folder, output_folders, txt_file_path)
    print(f"\nTotal missing files: {len(missing)}")