import os
from ruamel.yaml import YAML
from typing import List

# This file replaces the exposed_ip field in all yaml files under the experiments directory
# with the local_bind_ip field. This is useful for local testing where the exposed_ip
# is not reachable.


def get_yaml_file_paths(root_dir: str) -> List[str]:
    yaml_files = []

    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.yaml') or filename.endswith('.yml'):
                full_path = os.path.join(dirpath, filename)
                yaml_files.append(full_path)
    return yaml_files

def swap_exposed_ip_in_yaml_file(file_path: str):
    try:
        yaml = YAML()
        with open(file_path, 'r') as f:
            data = yaml.load(f)
            
    except FileNotFoundError:
        print(f"❌ Error: The file '{file_path}' was not found.")
        return
    except Exception as e:
        print(f"❌ An error occurred during file reading: {e}")
        return

    if 'endpoints' in data and isinstance(data['endpoints'], list):
        print(f"🔄 Processing {len(data['endpoints'])} endpoints in {file_path}...")
        
        for endpoint in data['endpoints']:
            if 'exposed_ip' in endpoint and 'local_bind_ip' in endpoint:
                new_exposed_ip = endpoint['local_bind_ip']
                endpoint['exposed_ip'] = new_exposed_ip
        with open(file_path, 'w') as f:
            yaml.dump(data, f)
        print(f"✅ Replacement complete. File '{file_path}' has been updated.")
    else:
        print(f"❌ Error: YAML structure in '{file_path}' is missing the 'endpoints' key or it is not a list.")


PATH = "../experiments"

paths = get_yaml_file_paths(PATH)

for path in paths:
    swap_exposed_ip_in_yaml_file(path)
    # indent_lines_after_first(path)

