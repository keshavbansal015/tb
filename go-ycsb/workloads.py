import os
from typing import Dict, Any, List, Tuple

# Constants
TOTAL_DATA_SIZE_BYTES = 256 * 1024 * 1024       # 256 MB
OPERATION_COUNT_LOW_BLOCK = 1000000             # 2 Million, 4kb*1000000 = 4GB
OPERATION_COUNT_STANDARD_BLOCK = 500000          # 500K, 8kb*500000 = 4GB
OPERATION_COUNT_MEDIUM_BLOCK = 5000        # 1 Million, 1MB*5000 = 5GB
OPERATION_COUNT_HIGH_BLOCK = 1000             # 500K, 8MB*1000 = 8GB

BLOCK_SIZE_THRESHOLD_BYTES = 4096               # 4 KB
BLOCK_SIZE_THRESHOLD_MEDIUM_BYTES = 8192                # 8 KB
BLOCK_SIZE_THRESHOLD_STANDARD_BYTES = 1048576             # 1 KB
BLOCK_SIZE_THRESHOLD_LOW_BYTES = 1048576*4 # 512 B

# Block sizes (in bytes) representing the requested range
# [128B, 512B, 1KB, 4KB, 8KB, 1MB, 2MB, 4MB, 8MB]
BLOCK_SIZES_BYTES = [
    128,          # 128 B
    512,          # 512 B
    1024,         # 1 KB
    4096,         # 4 KB (The threshold)
    8192,         # 8 KB
    1048576,      # 1 MB
    1048576*2,    # 2 MB 
    1048576*4,    # 4 MB
]

# Define the base properties common to all workloads
BASE_PROPERTIES = {
    "# Configuration for Record and Field": "",
    "fieldcount": "1",
    "fieldnames": "field0",
    "workload": "core",
    "insertorder": "hashed",
    
    "# Custom Record Count (Based on 16GB / Block Size)": "",
    "recordcount": "",  # Will be calculated
    "operationcount": "",  # Will be set based on block size
    "threadcount": "1",
    
    "# Operation Mix (Read-Only or Update-Only)": "",
    "readproportion": "",
    "updateproportion": "",
    "scanproportion": "0.0",
    "insertproportion": "0.0",

    "fieldlength": "",  # Will be set to block size
    
    "# Distribution (Fixed to uniform for this experiment)": "",
    "requestdistribution": "uniform",
    "zipfianconstant": "0.0"
}

def generate_properties_file(filepath: str, properties: Dict[str, Any]):
    """Writes the dictionary of properties to a YCSB .properties file."""
    # Ensure the directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Sort keys to ensure consistent file order, placing comments first
    # Simple list of keys to ensure order: Comments, field, record, op_mix, distribution
    ordered_keys = list(BASE_PROPERTIES.keys())
    # print(ordered_keys)
    with open(filepath, 'w') as f:
        # Write keys in the desired order
        for key in ordered_keys:
            value = properties.get(key)
            if key.startswith('#'):
                # Write comments or section headers
                f.write(f"\n{value}\n")
            elif value is not None and value != "":
                # Write key=value pair
                f.write(f"{key}={value}\n")

    print(f"Generated: {os.path.basename(filepath)}")


def format_block_size(size_bytes: int) -> str:
    """Formats block size for filename (e.g., 1024 -> 1KB, 8388608 -> 8MB)."""
    if size_bytes >= 1048576 and size_bytes % 1048576 == 0:
        return f"{size_bytes // 1048576}MB"
    elif size_bytes >= 1024 and size_bytes % 1024 == 0:
        return f"{size_bytes // 1024}KB"
    else:
        return f"{size_bytes}B"


def generate_custom_workloads(output_dir: str):
    """
    Generates workloads based on 16GB total size, varying block size, and 
    switching between read-only and update-only.
    """
    print("\n--- Generating Custom Workloads (16GB Data Size) ---")
    
    # Define the two operation mixes
    op_mixes: List[Tuple[str, str, str]] = [
        ("read", "1.0", "0.0"),  # (Name, readproportion, updateproportion)
        ("update", "0.0", "1.0"),
    ]

    for block_size_bytes in BLOCK_SIZES_BYTES:
        
        # 1. Calculate Record Count: 16 GB / block_size_bytes
        record_count = TOTAL_DATA_SIZE_BYTES // block_size_bytes + TOTAL_DATA_SIZE_BYTES % block_size_bytes
        
        # 2. Set Operation Count based on 4KB threshold
        if block_size_bytes <= BLOCK_SIZE_THRESHOLD_BYTES:
            op_count = OPERATION_COUNT_LOW_BLOCK
        elif block_size_bytes <= BLOCK_SIZE_THRESHOLD_MEDIUM_BYTES:
            op_count = OPERATION_COUNT_STANDARD_BLOCK
        elif block_size_bytes <= BLOCK_SIZE_THRESHOLD_STANDARD_BYTES:
            op_count = OPERATION_COUNT_MEDIUM_BLOCK
        else:
            op_count = OPERATION_COUNT_HIGH_BLOCK
            
        # 3. Format block size for file naming
        block_size_label = format_block_size(block_size_bytes)
        
        for op_name, read_prop, update_prop in op_mixes:
            
            # 4. Set filename convention: workload_{block_size}_{read or update}
            filename = f"workload_{block_size_label}_{op_name}.properties"
            filepath = os.path.join(output_dir, filename)
            
            # Start with base properties
            props = BASE_PROPERTIES.copy()
            
            # --- Override Calculated and Custom Values ---
            
            # Header
            props['# Configuration for Record and Field'] = \
                f"# Custom Workload: {block_size_label} size, {op_name}-only"
            
            # Block Size / Field Length
            props['fieldlength'] = str(block_size_bytes)
            
            # Record Count
            props['recordcount'] = str(record_count)
            
            # Operation Count
            props['operationcount'] = str(op_count)
            
            # Operation Mix
            props['readproportion'] = read_prop
            props['updateproportion'] = update_prop
            
            # Unique Table Assignment
            table_name = f"usertable_{block_size_label}_{op_name}"
            props['table'] = table_name

            # Field Length
            props['fieldlength'] = block_size_bytes

            # Generate the file
            generate_properties_file(filepath, props)


if __name__ == "__main__":
    # Define a clear output directory for the custom workloads
    OUTPUT_DIR = "custom_workloads"
    
    print(f"Creating workload files in: {OUTPUT_DIR}/")
    
    # Run the new custom generator
    generate_custom_workloads(OUTPUT_DIR)
    
    print("\nAll custom YCSB workload files have been generated.")
    
    # NOTE: The original script's functions (generate_size_workloads and
    # generate_distribution_workloads) were removed from the execution 
    # block to focus on the new custom task.
