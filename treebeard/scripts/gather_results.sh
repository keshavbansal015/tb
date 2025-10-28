#!/bin/bash

# --- Configuration ---
# Set the root directory where the experiments subdirectories start (e.g., ../experiments/read_ops)
# Using '..' assumes the script is run from inside the 'experiments' folder (e.g., read_ops).
ROOT_DIR="../experiments/" 

# Set the single, flat directory where all experiment logs will be copied.
OUTPUT_DIR="./extracted_logs"

# --- Execution ---

echo "Starting extraction of experiment_*.txt files from $ROOT_DIR..."

# 1. Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# 2. Use find to locate all files matching the pattern, and copy them
# The '-printf' action formats the output to generate the destination path, 
# ensuring unique names by including the parent directory structure.

# Example: mac_1_128B/experiment_1.txt -> extracted_logs/mac_1_128B__experiment_1.txt
find "$ROOT_DIR" -type f -name "experiment_*.txt" -exec sh -c '
    # $0 is the path to the found file (e.g., ./mac_1_128B/experiment_1.txt)
    SOURCE_PATH="$0"
    
    # Remove the starting dot/slash (e.g., mac_1_128B/experiment_1.txt)
    SOURCE_CLEAN=${SOURCE_PATH#./}
    
    # Replace slashes with double underscores for unique filename (e.g., mac_1_128B__experiment_1.txt)
    DEST_FILENAME=$(echo "$SOURCE_CLEAN" | tr "/" "__")
    
    # Copy the file to the output directory with the new name
    cp "$SOURCE_PATH" "$1/$DEST_FILENAME"
    echo "Extracted: $DEST_FILENAME"
' {} "$OUTPUT_DIR" \;

echo "--------------------------------------------------------"
echo "Extraction complete. All logs are in $OUTPUT_DIR"
