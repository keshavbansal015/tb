import os
import shutil
from pathlib import Path
from typing import List

# Directory where the ORAM configuration files are located
ORAM_CONFIGS_DIR = "./oram_configs/"

# Base directory for all experiments
DEST_BASE = "../experiments/"

# List of block size labels derived from your config files
BLOCK_SIZE_LABELS: List[str] = [
    "128B", "256B", "512B", "1KB", "2KB", "4KB", "8KB", 
    "1MB", "2MB", "4MB"]

def execute_config_move():
    """
    Copies the relevant ORAM configuration file into every corresponding 
    experiment directory structure.
    """
    print("--- Starting ORAM Config File Movement ---")

    source_dir = Path(ORAM_CONFIGS_DIR)

    if not source_dir.is_dir():
        print(f"🛑 Error: Source configuration directory not found at '{source_dir}'.")
        return

    moved_count = 0
    
    # Iterate through all necessary experiment dimensions
    for block_size_label in BLOCK_SIZE_LABELS:
        
        # Source file name is fixed based on the block size label
        source_filename = f"oram_config_{block_size_label}.yaml"
        source_file = source_dir / source_filename

        if not source_file.is_file():
            print(f"⚠️ Warning: Source config file not found: {source_file}. Skipping.")
            continue
            
        for machines in [1, 2, 3]:
            for op in ['read', 'write']:
                
                # 1. Define destination path based on the logic from move_trace.py
                op_folder = f"{op}_ops"
                dest_path_str = DEST_BASE + f"{op_folder}/mac_{machines}_{block_size_label}/"
                
                dest_dir = Path(dest_path_str)
                # 2. Ensure destination directory exists
                os.makedirs(dest_dir, exist_ok=True)
                
                # 3. Define the final file name in the destination
                dest_file = dest_dir / "parameters.yaml"
                
                try:
                    # 4. Copy the config file to the destination
                    shutil.copy(source_file, dest_file)
                    # print(f"✅ Copied: {source_filename} -> {dest_file}")
                    moved_count += 1
                except Exception as e:
                    print(f"❌ Error copying {source_file.name} to {dest_file}: {e}")

    print(f"--- Copy operation finished. Total files copied: {moved_count} ---")


if __name__ == "__main__":
    execute_config_move()
