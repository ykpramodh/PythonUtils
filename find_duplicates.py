# python3 find_duplicates.py /Users/yk/toshiba rescan
# python3 find_duplicates.py /path/to/directory rescan --min_size_mb 10
# python3 find_duplicates.py /path/to/directory load_from_pickle
# 

import os
import hashlib
import pickle
import argparse
from collections import defaultdict
from tqdm import tqdm

def calculate_checksum(filepath):
    """Calculate the checksum of the first 1000 characters of a file."""
    try:
        with open(filepath, 'rb') as f:
            data = f.read(1000)
            return hashlib.md5(data).hexdigest()
    except Exception as e:
        print(f"Error calculating checksum for {filepath}: {e}")
        return None

def find_duplicates(root_directory, min_size_mb):
    """Traverse all files in subfolders, find potential duplicates by size and name, and confirm with checksum."""
    files_data = defaultdict(list)
    min_size_bytes = min_size_mb * 1024 * 1024  # Convert MB to bytes

    # Get total number of files to process for progress bar
    total_files = sum(len(files) for _, _, files in os.walk(root_directory))
    
    # First pass: Group files by (size, name) with progress bar
    with tqdm(total=total_files, desc="Scanning files", unit="file") as pbar:
        for dirpath, _, filenames in os.walk(root_directory):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                # Only process if it's a file and meets the minimum size requirement
                if os.path.isfile(filepath):
                    file_size = os.path.getsize(filepath)
                    if file_size >= min_size_bytes:
                        files_data[(file_size, filename)].append(filepath)
                pbar.update(1)

    # Second pass: Calculate checksums for files with matching size and name
    duplicates = defaultdict(list)
    for (file_size, filename), paths in files_data.items():
        if len(paths) > 1:  # Only check groups with more than one file
            checksums = defaultdict(list)
            for path in paths:
                checksum = calculate_checksum(path)
                if checksum:
                    checksums[checksum].append(path)
            for checksum, dup_paths in checksums.items():
                if len(dup_paths) > 1:
                    duplicates[(checksum, file_size)].extend(dup_paths)

    return duplicates

def calculate_extra_data(duplicates):
    """Calculate the total extra data stored in duplicates."""
    extra_data = 0
    for (checksum, size), paths in duplicates.items():
        # Extra data is total size minus one instance (size * (num_duplicates - 1))
        extra_data += size * (len(paths) - 1)
    return extra_data

def save_to_pickle(data, filename):
    """Save data to a pickle file."""
    with open(filename, 'wb') as f:
        pickle.dump(data, f)
    print(f"Results saved to {filename}")

def load_from_pickle(filename):
    """Load data from a pickle file."""
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        print(f"No existing data found in {filename}")
        return None

# Main function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Duplicate file finder with checksum comparison.")
    parser.add_argument("root_directory", help="The root directory to scan for duplicate files.")
    parser.add_argument("action", choices=["load_from_pickle", "rescan"], help="Specify whether to load from pickle or rescan the directory.")
    parser.add_argument("--min_size_mb", type=int, default=5, help="Minimum file size in MB for scanning. Default is 5 MB.")
    args = parser.parse_args()

    # Create a pickle filename based on the root directory
    sanitized_path = args.root_directory.strip('/').replace('/', '_')
    pickle_file = f'duplicate_files_{sanitized_path}.pkl'
    
    if args.action == "load_from_pickle":
        # Load duplicates from pickle
        duplicates = load_from_pickle(pickle_file)
        if duplicates is None:
            print("No saved data found; please run with 'rescan' to generate data.")
    elif args.action == "rescan":
        # Rescan the directory with specified minimum size and save results to pickle
        duplicates = find_duplicates(args.root_directory, args.min_size_mb)
        save_to_pickle(duplicates, pickle_file)

    # Display results
    if duplicates:
        print("Duplicate files found:")
        for (checksum, size), paths in duplicates.items():
            print(f"\nChecksum: {checksum}, Size: {size} bytes")
            for path in paths:
                print(f" - {path}")
        
        # Calculate and display the extra data stored as duplicates
        extra_data = calculate_extra_data(duplicates)
        print(f"\nTotal extra data stored as duplicates: {extra_data / (1024 * 1024):.2f} MB")
    else:
        print("No duplicate files found.")



