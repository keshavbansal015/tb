import os

def append_to_hosts_files(start_dir, content_to_append):
    """
    Recursively searches a directory for files named 'hosts' and appends 
    a specified line to the end of each found file.
    """
    print(f"Starting search in directory: {os.path.abspath(start_dir)}")
    
    # Traverse the directory tree rooted at start_dir
    for root, _, files in os.walk(start_dir):
        if "hosts" in files:
            hosts_filepath = os.path.join(root, "hosts")
            
            try:
                # Open the file in append mode ('a')
                # We add a newline character (\n) before the content to ensure 
                # it starts on a new line, regardless of the previous file content.
                with open(hosts_filepath, 'a') as f:
                    f.write('\n' + content_to_append + '\n')
                
                print(f"SUCCESS: Appended to {hosts_filepath}")
                
            except PermissionError:
                print(f"ERROR: Permission denied for {hosts_filepath}. Skipping.")
            except Exception as e:
                print(f"ERROR: Could not modify {hosts_filepath}. Details: {e}")

# --- Configuration ---
# Set the directory where the search should begin. 
# '.' means the current working directory.
START_DIRECTORY = '.' 
LINE_TO_APPEND = "host0 ansible_host=30.0.0.1 ansible_user=cc"

if __name__ == "__main__":
    append_to_hosts_files(START_DIRECTORY, LINE_TO_APPEND)
