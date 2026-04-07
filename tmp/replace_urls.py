import sys
import os

def replace_in_file(filename):
    if not os.path.exists(filename):
        print(f"File {filename} not found")
        return

    with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # Define replacements
    replacements = [
        ('http://localhost:7860', ''),
        ('http://127.0.0.1:7860', ''),
        ('http://localhost:4000', ''),
        ('http://127.0.0.1:4000', ''),
        ('http://localhost:3000', ''),
        ('http://127.0.0.1:3000', ''),
    ]
    
    # Perform replacements
    new_content = content
    for old, new in replacements:
        new_content = new_content.replace(old, new)
    
    if new_content != content:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated {filename}")
    else:
        print(f"No changes in {filename}")

if __name__ == "__main__":
    for arg in sys.argv[1:]:
        replace_in_file(arg)
