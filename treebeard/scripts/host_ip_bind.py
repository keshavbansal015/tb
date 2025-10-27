import os
import shutil
import json
from typing import Dict, List, Tuple
from ruamel.yaml import YAML

# This file traverses the directory structure under 'treebeard/experiments/',
# extracts host mappings from YAML files, and generates local 'hosts' files
# for Ansible as well as a global JSON database of all host mappings.


# Initialize the YAML processor once
# Using typ='safe' is efficient for reading structured data
yaml = YAML(typ='safe')

# --- Step 1 & 2: Get Experiment Directories and Extract Mappings ---

def get_experiment_data_mappings(root_dir: str) -> Dict[str, Dict[str, str]]:
    """
    Traverses the directory structure to find YAML files, groups them by their 
    parent directory (the experiment directory), and extracts host mappings 
    (deploy_host -> local_bind_ip) from all YAML files in that directory.
    
    Returns a dictionary mapping:
    {
        'path/to/experiment/dir': {
            'deploy_host_1': 'local_bind_ip_1',
            'deploy_host_2': 'local_bind_ip_2',
            ...
        },
        ...
    }
    """
    all_experiment_mappings = {}
    
    # Use a dictionary to group YAML files by their parent directory
    yaml_files_by_dir = {}

    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.yaml') or filename.endswith('.yml'):
                experiment_dir = dirpath
                if experiment_dir not in yaml_files_by_dir:
                    yaml_files_by_dir[experiment_dir] = []
                yaml_files_by_dir[experiment_dir].append(os.path.join(dirpath, filename))

    if not yaml_files_by_dir:
        print(f"🛑 No YAML files found in directory structure starting at: {root_dir}")
        return all_experiment_mappings
        
    print(f"Found data across {len(yaml_files_by_dir)} experiment directories.")

    # Process files within each experiment directory
    for experiment_dir, file_paths in yaml_files_by_dir.items():
        # This will hold the consolidated mappings for the current directory
        experiment_host_map = {}
        
        for file_path in file_paths:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = yaml.load(f)
            except Exception as e:
                print(f"❌ Could not load or parse YAML file: {file_path}. Error: {e}")
                continue

            # Check for the 'endpoints' list in the YAML structure
            if isinstance(data, dict) and 'endpoints' in data and isinstance(data['endpoints'], list):
                
                for endpoint in data['endpoints']:
                    host = endpoint.get('deploy_host')
                    ip = endpoint.get('local_bind_ip')
                    
                    if host and ip:
                        # Add mapping to the current experiment's map, overwriting if duplicate is found 
                        # (assumes later files take precedence or are duplicates)
                        experiment_host_map[host] = ip
        
        if experiment_host_map:
            all_experiment_mappings[experiment_dir] = experiment_host_map
            print(f"   -> Extracted {len(experiment_host_map)} unique host mappings for: {experiment_dir}")
        else:
             print(f"   -> No valid host mappings found in YAML files for: {experiment_dir}")

    return all_experiment_mappings

# --- Step 3: Create Local 'hosts' File ---

def write_hosts_file(directory: str, host_map: Dict[str, str], ansible_user: str = 'cc'):
    """
    Creates or overwrites the 'hosts' file in the specified directory 
    using the Ansible inventory format.
    """
    hosts_file_path = os.path.join(directory, 'hosts')
    hosts_lines = []
    
    # Iterate through the collected mappings (deploy_host: local_bind_ip)
    for host, ip in host_map.items():
        # Format the line as requested: hostX ansible_host=ipX ansible_user=cc
        line = f"{host} ansible_host={ip} ansible_user={ansible_user}"
        hosts_lines.append(line)
        
    try:
        with open(hosts_file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(hosts_lines) + '\n') # Add final newline
        print(f"   -> Generated local hosts file: {hosts_file_path}")
    except Exception as e:
        print(f"❌ Failed to write hosts file to {hosts_file_path}. Error: {e}")

# --- Step 4 & Main Orchestration ---

def process_and_generate_config_files(root_dir: str):
    """
    Main function to orchestrate path gathering, mapping extraction, and file generation.
    """
    
    # 1. Get mappings grouped by experiment directory
    experiment_data = get_experiment_data_mappings(root_dir)
    
    if not experiment_data:
        print("Processing finished: No host data found.")
        return

    # Initialize the global database (combines all mappings)
    global_host_database = {}

    for directory, host_map in experiment_data.items():
        # A. Write the local 'hosts' file for the experiment directory
        write_hosts_file(directory, host_map)
        
        # B. Add local map to the global database
        # Using the relative directory name as the key for clarity
        global_host_database[directory] = host_map
        
    # C. Write the final global database file (JSON format)
    global_file_path =  './global_hosts_map.json'
    try:
        with open(global_file_path, 'w', encoding='utf-8') as f:
            json.dump(global_host_database, f, indent=4)
        print(f"\n✨ Successfully generated global database: {global_file_path}")
    except Exception as e:
        print(f"❌ Failed to write global hosts database. Error: {e}")

    print("\nProcessing complete.")


# --- Mock Setup and Execution ---

def create_mock_directory_structure(root_dir):
    """
    Creates a mock directory structure mimicking the user's setup for testing.
    The content is placed in two different YAML files per experiment.
    """
    if os.path.exists(root_dir):
        shutil.rmtree(root_dir)
    os.makedirs(root_dir)

    mock_structure = [
        # Experiment 1: example/functional/ (Two YAML files)
        (f'{root_dir}/example/functional/oramnode_endpoints.yaml', 
         """endpoints:
  - exposed_ip: 52.235.62.184
    local_bind_ip: 10.0.0.8
    deploy_host: host1
    port: 8845
  - exposed_ip: 52.235.56.209
    local_bind_ip: 10.0.0.9
    deploy_host: host2
    port: 8845
"""),
        (f'{root_dir}/example/functional/router_endpoints.yaml', 
         """endpoints:
  - exposed_ip: 52.235.58.158
    local_bind_ip: 10.0.0.10
    deploy_host: router1
    port: 8745
"""),
        # Experiment 2: paper_blocksize_experiments/1024/ (One YAML file)
        (f'{root_dir}/paper_blocksize_experiments/1024/shardnode_endpoints.yaml', 
         """endpoints:
  - exposed_ip: 1.1.1.1
    local_bind_ip: 192.168.0.100
    deploy_host: shard1
  - exposed_ip: 2.2.2.2
    local_bind_ip: 192.168.0.101
    deploy_host: shard2
"""),
        # Experiment 3: paper_dist_experiments/uniform/ (One YAML file)
        (f'{root_dir}/paper_dist_experiments/uniform/parameters.yaml', 
         """settings:
  distribution: uniform
endpoints:
  - exposed_ip: 3.3.3.3
    local_bind_ip: 172.16.0.1
    deploy_host: dist_host_A
""")
    ]
    
    for full_path, content in mock_structure:
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
    print(f"Mock directory structure created at: {root_dir}")
    print("-------------------------------------------------------")


if __name__ == '__main__':
    ROOT_DIR = './treebeard/experiments/'
    
    # Setup mock environment
    # create_mock_directory_structure(ROOT_MOCK_DIR)
    
    # Run the main processing function
    # process_and_generate_config_files(ROOT_MOCK_DIR)
    process_and_generate_config_files(ROOT_DIR)

    # Verification: Read and print one of the generated hosts files
    # print("\n--- Verification: Contents of {ROOT_DIR} ---")
    # hosts_file_path = ROOT_DIR + 'example/functional/hosts'
    # with open(hosts_file_path, 'r', encoding='utf-8') as f:
    #     print(f.read())

    # Verification: Read and print the global JSON file
    print("--- Verification: Contents of global_hosts_map.json ---")
    json_file_path = './global_hosts_map.json'
    with open(json_file_path, 'r', encoding='utf-8') as f:
        print(f.read())
        
    # Cleanup mock environment
    # shutil.rmtree(ROOT_MOCK_DIR)
