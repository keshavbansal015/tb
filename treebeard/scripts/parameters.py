import os
import math
from typing import Dict, Any, List

# --- Constants imported from the YCSB Canvas for consistency ---
TOTAL_DATA_SIZE_BYTES = 16 * 1024 * 1024 * 1024  # 16 GB
# Block sizes (in bytes) to iterate through
BLOCK_SIZES_BYTES: List[int] = [
    128,          # 128 B
    512,          # 512 B
    1024,         # 1 KB
    4096,         # 4 KB
    8192,         # 8 KB
    1048576,      # 1 MB
    1048576*2,    # 2 MB
    1048576*4,    # 4 MB
    8388608       # 8 MB
]
# -------------------------------------------------------------

# Base configuration based on the user's provided structure
BASE_CONFIG: Dict[str, Any] = {
    "max-blocks-to-send": 400,
    "eviction-rate": 100,
    "evict-path-count": 200,
    "batch-timeout": 1,
    "epoch-time": 1,
    "trace": "true",
    "Z": 1,
    "S": 4,
    "shift": 1,
    "tree-height": 0,    # Calculated value
    "redis-pipeline-size": 3000000,
    "max-requests": 15000,
    "block-size": 0,     # Calculated value
    "log": "false",
    "profile": "false"
}

def format_block_size(size_bytes: int) -> str:
    """Formats block size for filename (e.g., 1024 -> 1KB, 8388608 -> 8MB)."""
    if size_bytes >= 1048576 and size_bytes % 1048576 == 0:
        return f"{size_bytes // 1048576}MB"
    elif size_bytes >= 1024 and size_bytes % 1024 == 0:
        return f"{size_bytes // 1024}KB"
    else:
        return f"{size_bytes}B"

def generate_config_file(filepath: str, config: Dict[str, Any]):
    """Writes the dictionary of configuration values to a simple key: value file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w') as f:
        f.write(f"# ORAM Configuration for Block Size: {config['block-size']} bytes\n")
        f.write(f"# Tree Height calculated as ceil(log2(16GB / block-size)) = {config['tree-height']}\n")
        
        # Write properties in the specified order
        for key, value in BASE_CONFIG.items():
            # Use the actual calculated value from the config dictionary
            actual_value = config.get(key, value)
            f.write(f"{key}: {actual_value}\n")
            
    print(f"Generated: {os.path.basename(filepath)}")

def calculate_tree_height(block_size_bytes: int) -> int:
    """Calculates tree height H = ceil(log2(N/B)), where N=16GB and B=block_size."""
    
    # L = Number of blocks required to store 16GB
    num_blocks = math.ceil(TOTAL_DATA_SIZE_BYTES / block_size_bytes)
    
    # H = ceil(log2(L))
    if num_blocks > 0:
        height = math.ceil(math.log2(num_blocks))
    else:
        height = 0 # Should not happen with valid block size
        
    # The result must be an integer
    return int(height)

def generate_oram_workloads(output_dir: str):
    """
    Generates ORAM parameter files varying block-size and calculating tree-height.
    """
    print("\n--- Generating ORAM Config Files ---")
    
    for block_size in BLOCK_SIZES_BYTES:
        
        # 1. Calculate Tree Height
        tree_height = calculate_tree_height(block_size)
        
        # 2. Set file naming
        block_size_label = format_block_size(block_size)
        filename = f"oram_config_{block_size_label}.yaml"
        filepath = os.path.join(output_dir, filename)
        
        # 3. Create the configuration dictionary
        config = BASE_CONFIG.copy()
        config["block-size"] = block_size
        config["tree-height"] = tree_height
        
        # 4. Generate the file
        generate_config_file(filepath, config)


if __name__ == "__main__":
    OUTPUT_DIR = "oram_configs"
    
    print(f"Creating ORAM config files in: {OUTPUT_DIR}/")
    
    generate_oram_workloads(OUTPUT_DIR)
    
    print("\nAll ORAM configuration files have been generated.")
