import os
import shutil
from pathlib import Path
from typing import Dict

# --- Configuration ---

# Directory where your raw YCSB results are currently stored
YCSB_RESULTS_DIR = "go-ycsb"
MOCK_DATA_PREFIX = "mock_data"

# The mapping from destination directory path (Key) to the base source file name (Value).
# This structure is necessary to avoid duplicate keys overwriting previous mappings.
# Source file will be: f"{YCSB_RESULTS_DIR}/{workload_name}_result.txt"
# Destination file will be: f"{destination_path}/results/{workload_name}_result.txt"
WORKLOAD_MAP: Dict[str, str] = {
    # --- Set 1: Blocksize Experiments ---
    "./treebeard/experiments/paper_blocksize_experiments/128": "usertable_size_128B",
    "./treebeard/experiments/paper_blocksize_experiments/256": "usertable_size_256B",
    "./treebeard/experiments/paper_blocksize_experiments/512": "usertable_size_512B",
    "./treebeard/experiments/paper_blocksize_experiments/1024": "usertable_size_1024B",
    
    # --- Set 2: Distribution Experiments ---
    "./treebeard/experiments/paper_dist_experiments/uniform": "usertable_dist_uniform",
    "./treebeard/experiments/paper_dist_experiments/zipf0_2": "usertable_dist_zipf2",
    "./treebeard/experiments/paper_dist_experiments/zipf0_4": "usertable_dist_zipf4",
    "./treebeard/experiments/paper_dist_experiments/zipf0_6": "usertable_dist_zipf6",
    "./treebeard/experiments/paper_dist_experiments/zipf0_8": "usertable_dist_zipf8",
    "./treebeard/experiments/paper_dist_experiments/zipf0_99": "usertable_dist_zipf99",

    # --- Set 3: Experiments sharing "usertable_dist_uniform" ---
    "./treebeard/experiments/paper_failure_experiments/oram": "usertable_dist_uniform",
    "./treebeard/experiments/paper_failure_experiments/shard": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc700": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc8000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc300": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc900": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc9000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc6000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc400": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc3000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc7000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc2500": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc500": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc100": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc4000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc5000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc1000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc600": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc800": "usertable_dist_uniform",
    "./treebeard/experiments/paper_quoram_experiments/conc200": "usertable_dist_uniform",
    "./treebeard/experiments/example/functional": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/3machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/10machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/2machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/8machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/12machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/4machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/16machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/5machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/7machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/14machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_scaling_experiments/6machines": "usertable_dist_uniform",
    "./treebeard/experiments/paper_epoch_experiments/0_5": "usertable_dist_uniform",
    "./treebeard/experiments/paper_epoch_experiments/20": "usertable_dist_uniform",
    "./treebeard/experiments/paper_epoch_experiments/1": "usertable_dist_uniform",
    "./treebeard/experiments/paper_epoch_experiments/10": "usertable_dist_uniform",
    "./treebeard/experiments/paper_epoch_experiments/0_1": "usertable_dist_uniform",
    "./treebeard/experiments/paper_epoch_experiments/2": "usertable_dist_uniform",
    "./treebeard/experiments/paper_epoch_experiments/5": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A1K2000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A100K5000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A10K200": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A10K2000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A1K10000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A100K200": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A10K5000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A100K2000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A1K5000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A10K10000": "usertable_dist_uniform",
    "./treebeard/experiments/paper_stash_experiments_v2/A1K200": "usertable_dist_uniform",
    "./treebeard/experiments/paper_per_layer_experiments/r2_sh3_om3_red6": "usertable_dist_uniform",
    "./treebeard/experiments/paper_per_layer_experiments/r3_sh3_om2_red6": "usertable_dist_uniform",
    "./treebeard/experiments/paper_per_layer_experiments/r3_sh3_om3_red6": "usertable_dist_uniform",
    "./treebeard/experiments/paper_per_layer_experiments/r3_sh2_om3_red6": "usertable_dist_uniform",
    "./treebeard/experiments/paper_per_layer_experiments/r3_sh1_om3_red6": "usertable_dist_uniform",
    "./treebeard/experiments/paper_per_layer_experiments/r1_sh3_om3_red6": "usertable_dist_uniform",
    "./treebeard/experiments/paper_per_layer_experiments/r3_sh3_om1_red6": "usertable_dist_uniform",
}


def execute_mapped_move():
    """
    Iterates through the WORKLOAD_MAP, checks for the corresponding YCSB result file,
    creates the destination directory structure, and moves the file.

    Args:
        use_mock_prefix: If True, prefixes all destination paths with MOCK_DATA_PREFIX.
    """
    print("--- Starting Mapped File Movement ---")
    
    ycsb_dir = Path(YCSB_RESULTS_DIR)
    
    if not ycsb_dir.is_dir():
        print(f"🛑 Error: YCSB results directory not found at '{ycsb_dir}'.")
        print("Please ensure your raw result files are in that directory.")
        return

    moved_count = 0
    
    # Iterate over the map: key is the destination path, value is the source file base name
    for dest_path_str, workload_name in WORKLOAD_MAP.items():
        # 1. Define source and destination paths using pathlib
        source_file = ycsb_dir / f"{workload_name}"

        # Apply mock prefix if needed
        base_dest_path = Path(dest_path_str)

        # Destination directory is BASE_PATH/results/
        dest_dir = base_dest_path
        # Destination file retains the original file name
        dest_file = dest_dir / "trace.txt"

        if source_file.is_file():
            try:
                
                # NOTE: If multiple destinations use the SAME source file, the file will only be moved once.
                # If you need to COPY the file, change shutil.move to shutil.copy
                shutil.copy(source_file, dest_file)
                print(f"✅ Moved: {source_file.name} -> {dest_file}")
                
                moved_count += 1
            except Exception as e:
                print(f"❌ Error moving {source_file.name} to {dest_file}: {e}")
        else:
            # Note: This warning will show up multiple times if a single missing source file
            # is mapped to multiple destinations (like 'usertable_dist_uniform').
            print(f"⚠️ Warning: Source result file not found: {source_file} (Skipping destination: {dest_path_str})")
        
    print(f"--- Move operation finished. Total files moved: {moved_count} ---")


def setup_mock_environment():
    """Sets up temporary files and directories to test the move script."""
    print("--- Setting Up Mock Environment ---")
    
    # 1. Cleanup previous runs
    if Path(MOCK_DATA_PREFIX).is_dir():
        shutil.rmtree(MOCK_DATA_PREFIX)
    if Path(YCSB_RESULTS_DIR).is_dir():
        shutil.rmtree(YCSB_RESULTS_DIR)

    Path(YCSB_RESULTS_DIR).mkdir(exist_ok=True)
    
    # 2. Create mock destination folders (using the mock prefix)
    for path_str in WORKLOAD_MAP.keys():
        mock_path = Path(MOCK_DATA_PREFIX) / Path(path_str)
        mock_path.mkdir(parents=True, exist_ok=True)

    # 3. Create mock YCSB result files based on the unique values (workload names)
    unique_workloads = set(WORKLOAD_MAP.values())
    file_count = 0
    
    for workload_name in unique_workloads:
        source_path = Path(YCSB_RESULTS_DIR) / f"{workload_name}_result.txt"
        source_path.write_text(f"Mock YCSB result for {workload_name}")
        file_count += 1
    
    print(f"Mock environment created:")
    print(f"  - Destination paths created under '{MOCK_DATA_PREFIX}/'")
    print(f"  - {file_count} unique source files created in '{YCSB_RESULTS_DIR}/'")
    print("-----------------------------------")


def cleanup_mock_environment():
    """Cleans up the directories created for mock testing."""
    if Path(MOCK_DATA_PREFIX).is_dir():
        shutil.rmtree(MOCK_DATA_PREFIX)
    if Path(YCSB_RESULTS_DIR).is_dir():
        shutil.rmtree(YCSB_RESULTS_DIR)
    print("--- Clean up complete. ---")


if __name__ == "__main__":
    # --- Script Execution ---
    
    # Setup mock environment for testing the file movement logic
    # setup_mock_environment()
    
    # Execute the move operation, using the mock prefix for testing safely
    # Set 'use_mock_prefix=False' to move files into the real paths
    execute_mapped_move()
    
    # Cleanup the mock environment after testing
    # cleanup_mock_environment() # Uncomment this line to auto-clean up after the run
