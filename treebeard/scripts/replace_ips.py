import json
import os
import shutil
from typing import Dict, List, Set, Tuple

# This file updates IP addresses in YAML and hosts files across experiment directories
# by creating a 1-to-1 mapping from original IPs to new IPs sourced
# from a provided list. The mapping is applied sequentially per experiment.
# This ensures that each experiment gets a consistent but unique set of IPs
# for local testing or deployment scenarios.

# --- Core Functions ---

def read_ip_list(ip_file_path: str) -> List[str]:
    """Reads a list of IP addresses from a plain text file."""
    try:
        with open(ip_file_path, 'r', encoding='utf-8') as f:
            # Filter out empty lines and strip whitespace
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"🛑 Error: IP list file not found at '{ip_file_path}'")
        return []

def read_global_db(global_db_path: str) -> Dict[str, Dict[str, str]]:
    """Reads the global JSON database mapping experiment paths to host maps."""
    try:
        with open(global_db_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"🛑 Error: Global database file not found at '{global_db_path}'")
        return {}
    except json.JSONDecodeError:
        print(f"❌ Error: Failed to parse JSON from '{global_db_path}'.")
        return {}

def replace_ips_in_experiment(experiment_dir: str, translation_map: Dict[str, str]):
    """
    Iterates through all YAML and hosts files in the directory and replaces 
    old IP strings with new IP strings using the experiment-specific translation map.
    """
    print(f"--- Applying map to experiment files: {os.path.basename(experiment_dir)} ---")
    
    # Get all YAML and hosts files in the directory
    files_to_update = [
        f for f in os.listdir(experiment_dir) 
        if f.endswith(('.yaml', '.yml', 'hosts')) and not f.startswith("j")
    ]
    
    if not files_to_update:
        print("   -> No YAML or hosts files found. Skipping file modification.")
        return

    # Process each file
    for filename in files_to_update:
        file_path = os.path.join(experiment_dir, filename)
        
        try:
            # 1. Read file content as a single string
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # 2. Perform string replacement
            replaced_count = 0
            for old_ip, new_ip in translation_map.items():
                # Only replace if the old IP exists to avoid unnecessary string operations
                if old_ip in content:
                    content = content.replace(old_ip, new_ip)
                    replaced_count += 1
            
            # 3. Write back only if content changed (saves file write operation)
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"   -> Updated IP addresses in: {filename}")
            # else: print(f"   -> No relevant IPs found in: {filename}")
                
        except Exception as e:
            print(f"❌ Failed to process file {filename}. Error: {e}")


def main_processor(root_dir: str, ip_list_filename: str, global_db_filename: str):
    """
    Orchestrates the entire IP update process by creating and applying 
    an experiment-specific translation map sequentially.
    """
    
    ip_list_path = ip_list_filename
    global_db_path = global_db_filename

    # 1. Load data sources
    # new_ips is loaded as a list so we can consume them sequentially using pop()
    new_ips = read_ip_list(ip_list_path)
    global_data = read_global_db(global_db_path)
    
    if not new_ips or not global_data:
        print("\nProcessing stopped due to missing source files or empty database.")
        return
        
    print(f"\nLoaded {len(new_ips)} IPs for mapping pool.")

    # 2. Iterate through each experiment and build a fresh map for it
    for experiment_dir, host_map in global_data.items():
        
        # Determine the unique IPs that need replacing in this experiment
        experiment_original_ips = set(host_map.values())
        
        # Sort the original IPs to ensure consistent assignment order
        sorted_original_ips = sorted(list(experiment_original_ips))
        
        # Build the experiment-specific translation map
        translation_map = {}
        
        num_original = len(sorted_original_ips)
        
        print(f"\nProcessing experiment: {experiment_dir} ({num_original} unique IPs)")
        
        # Create the 1-to-1 map by consuming IPs from the new_ips pool
        for i, old_ip in enumerate(sorted_original_ips):
            if i >= len(new_ips): # Check against the non-mutated list length
                print(f"⚠️ Warning: IP pool exhausted for this experiment. Skipping remaining {old_ip}.")
                break
                
            # Access by index (i) ensures the pool is NOT consumed and the index restarts at 0
            new_ip = new_ips[i] 
            translation_map[old_ip] = new_ip
            print(f"   Mapped: {old_ip} -> {new_ip}")
        
        # 3. Apply the translation map to the current experiment directory
        full_experiment_path = experiment_dir #os.path.join(root_dir, experiment_dir)
        
        if not os.path.isdir(full_experiment_path):
             print(f"⚠️ Warning: Directory not found, skipping file replacement: {full_experiment_path}")
             continue
             
        replace_ips_in_experiment(full_experiment_path, translation_map)
        
    print(f"\n✅ All IP replacement processing complete. {len(new_ips)} IPs remaining in the pool.")


# --- Mock Setup and Execution ---

def create_mock_directory_structure(root_dir: str, ip_list_filename: str, global_db_filename: str):
    """Creates a mock environment for testing the update process."""
    
    if os.path.exists(root_dir):
        shutil.rmtree(root_dir)
    os.makedirs(root_dir)
    
    # --- 1. Create the New IP List File ---
    # Total of 6 IPs available for sequential assignment
    new_ips_content = """
30.0.0.1
30.0.0.2
30.0.0.3
30.0.0.4
30.0.0.5
30.0.0.6
"""
    with open(os.path.join(root_dir, ip_list_filename), 'w') as f:
        f.write(new_ips_content)
        
    # --- 2. Create the Global Database File ---
    # Experiment 1 needs 3 unique IPs (112, 117, 119)
    # Experiment 2 needs 2 unique IPs (112, 120)
    global_data = {
        "experiments/dist_experiments/uniform": {
            "host1": "192.168.252.112",  
            "host2": "192.168.252.117",  
            "host3": "192.168.252.119",  
            "host4": "192.168.252.112",  
            "host5": "192.168.252.117",  
            "host6": "192.168.252.119"   
        },
        "experiments/block_experiments/256": {
            "hostA": "192.168.252.112", # NOTE: Same IP as host1 in Exp 1, but will get a NEW IP in Exp 2
            "hostB": "192.168.252.120"   
        }
    }
    with open(os.path.join(root_dir, global_db_filename), 'w') as f:
        json.dump(global_data, f, indent=4)
        
    # --- 3. Create Experiment Directory Files (YAML and Hosts) ---
    
    # Experiment 1: uniform
    exp1_dir = os.path.join(root_dir, "experiments/dist_experiments/uniform")
    os.makedirs(exp1_dir, exist_ok=True)
    
    # router_endpoints.yaml
    with open(os.path.join(exp1_dir, "router_endpoints.yaml"), 'w') as f:
        f.write("""endpoints:
 - exposed_ip: 192.168.252.112  # Mapped to 10.10.10.1
   local_bind_ip: 192.168.252.112
 - exposed_ip: 192.168.252.117  # Mapped to 10.10.10.2
   local_bind_ip: 192.168.252.117
 - exposed_ip: 192.168.252.119  # Mapped to 10.10.10.3
   local_bind_ip: 192.168.252.119
""")
    # hosts file
    with open(os.path.join(exp1_dir, "hosts"), 'w') as f:
        f.write("host1 ansible_host=192.168.252.112 ansible_user=cc\n")
        f.write("host2 ansible_host=192.168.252.117 ansible_user=cc\n")
        
    # Experiment 2: 256
    exp2_dir = os.path.join(root_dir, "experiments/block_experiments/256")
    os.makedirs(exp2_dir, exist_ok=True)
    
    # oramnode_endpoints.yaml
    with open(os.path.join(exp2_dir, "oramnode_endpoints.yaml"), 'w') as f:
        f.write("""endpoints:
 - local_bind_ip: 192.168.252.112  # Mapped to 10.10.10.4 (Sequential Consumption)
 - local_bind_ip: 192.168.252.120  # Mapped to 10.10.10.5
 - local_bind_ip: 192.168.252.112  # Mapped to 10.10.10.4 (Sequential Consumption)
 - local_bind_ip: 192.168.252.120  # Mapped to 10.10.10.5
""")
    
    print(f"Mock directory structure created in '{root_dir}'.")
    print("-------------------------------------------------------")

def verify_output(root_dir: str):
    """Prints the content of a file after processing for verification."""
    
    print("\n\n#######################################################")
    print("### VERIFICATION OF IP REPLACEMENT (SEQUENTIAL MAP) ###")
    print("#######################################################")
    
    print("\n--- Experiment 1: uniform/router_endpoints.yaml ---")
    exp1_path = os.path.join(root_dir, "experiments/dist_experiments/uniform/router_endpoints.yaml")
    try:
        with open(exp1_path, 'r') as f:
            print(f.read())
    except FileNotFoundError:
        print(f"File not found: {exp1_path}")        
    print("--- Experiment 2: 256/oramnode_endpoints.yaml ---")
    exp2_path = os.path.join(root_dir, "experiments/block_experiments/256/oramnode_endpoints.yaml")
    with open(exp2_path, 'r') as f:
        print(f.read())


if __name__ == '__main__':
    # if os.path.exists('temp_ip_update_data'):
        # shutil.rmtree('temp_ip_update_data')
    
    # ROOT_MOCK_DIR = 'temp_ip_update_data'
    ROOT_DIR = '../experiments'  # Change this to your actual root directory if needed
    IP_LIST_FILE = 'new_ip_pool.txt'
    GLOBAL_DB_FILE = 'global_hosts_map.json'
    
    # 1. Setup mock environment
    # create_mock_directory_structure(ROOT_MOCK_DIR, IP_LIST_FILE, GLOBAL_DB_FILE)
    
    # 2. Run the main processing function
    main_processor(ROOT_DIR, IP_LIST_FILE, GLOBAL_DB_FILE)
    
    # 3. Verify changes
    # verify_output(ROOT_DIR)

    # 4. Cleanup mock environment
    # shutil.rmtree(ROOT_MOCK_DIR)
