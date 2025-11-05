import os
import shutil

def move_files_from_list(txt_file_path, source_folder, destination_folder):
    """
    Read filenames from a text file and move those files from source to destination folder.
    
    Args:
        txt_file_path: Path to the text file containing filenames (one per line)
        source_folder: Folder where the files currently are
        destination_folder: Folder where files should be moved to
    
    Returns:
        Tuple of (successfully_moved_files, failed_files)
    """
    moved_files = []
    failed_files = []
    
    # Check if text file exists
    if not os.path.exists(txt_file_path):
        print(f"Error: Text file '{txt_file_path}' does not exist")
        return moved_files, failed_files
    
    # Check if source folder exists
    if not os.path.exists(source_folder):
        print(f"Error: Source folder '{source_folder}' does not exist")
        return moved_files, failed_files
    
    # Create destination folder if it doesn't exist
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
        print(f"Created destination folder: {destination_folder}")
    
    # Read filenames from text file
    try:
        with open(txt_file_path, 'r') as f:
            filenames = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading text file: {e}")
        return moved_files, failed_files
    
    print(f"\nAttempting to move {len(filenames)} files...\n")
    
    # Move each file
    for filename in filenames:
        source_path = os.path.join(source_folder, filename)
        destination_path = os.path.join(destination_folder, filename)
        
        # Check if file exists in source folder
        if not os.path.exists(source_path):
            print(f"⚠ File not found: {filename}")
            failed_files.append(filename)
            continue
        
        # Check if file already exists in destination
        if os.path.exists(destination_path):
            print(f"⚠ File already exists in destination: {filename}")
            failed_files.append(filename)
            continue
        
        # Move the file
        try:
            shutil.move(source_path, destination_path)
            print(f"✓ Moved: {filename}")
            moved_files.append(filename)
        except Exception as e:
            print(f"✗ Failed to move {filename}: {e}")
            failed_files.append(filename)
    
    # Summary
    print(f"\n{'='*50}")
    print(f"Summary:")
    print(f"  Successfully moved: {len(moved_files)} files")
    print(f"  Failed: {len(failed_files)} files")
    print(f"{'='*50}")
    
    return moved_files, failed_files


# Example usage
if __name__ == "__main__":
    # Example paths
    txt_file = "/Users/daniellanghann/src/api-showcase/api-showcase/missing_files.txt"
    source = "/Users/daniellanghann/src/api-showcase/api-showcase/src/api_showcase/rest_importer/test_documents/ALL"
    destination = "/Users/daniellanghann/src/api-showcase/api-showcase/src/api_showcase/rest_importer/test_documents/0_MISSING"
    
    moved, failed = move_files_from_list(txt_file, source, destination)
    
    # You can also save the results
    if moved:
        print(f"\nMoved files: {moved}")