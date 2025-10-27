import os
import shutil
from pathlib import Path

# Directory where your raw YCSB results are currently stored
YCSB_RESULTS_DIR = "/home/cc/tb/go-ycsb/ycsb_data/"
DEST_BASE = "../experiments/"
sizes = [128, 512, 1024, 4096, 8192, 1048576, 1048576*2, 1048576*4]

WORKLOAD_MAP = {}

def format_block_size(size_bytes: int) -> str:
    """Formats block size for filename (e.g., 1024 -> 1KB, 8388608 -> 8MB)."""
    if size_bytes >= 1048576 and size_bytes % 1048576 == 0:
        return f"{size_bytes // 1048576}MB"
    elif size_bytes >= 1024 and size_bytes % 1024 == 0:
        return f"{size_bytes // 1024}KB"
    else:
        return f"{size_bytes}B"


for size in sizes:
    for machines in [1, 2, 3]:
        for op in ['read', 'update']:
            if op == 'read':
                dest_path = DEST_BASE + f"read_ops/mac_{machines}_{format_block_size(size)}/"
            else:
                dest_path = DEST_BASE + f"write_ops/mac_{machines}_{format_block_size(size)}/"
            workload_name =  "usertable_{}_{}".format(format_block_size(size), op)
            WORKLOAD_MAP[dest_path] = workload_name


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
            print(
                f"⚠️ Warning: Source result file not found: {source_file} (Skipping destination: {dest_path_str})")

    print(f"--- Move operation finished. Total files moved: {moved_count} ---")


if __name__ == "__main__":
    execute_mapped_move()
