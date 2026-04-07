import re
import os
import sys

def fix_connector_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replacement patterns
    pats = [
        # redirect_uri = "http://localhost..."
        (r'redirect_uri\s*=\s*["\']https?://(?:localhost|127\.0\.0\.1)(?::\d+)?(/[^"\']*)["\']',
         r'redirect_uri = request.host_url.rstrip("/") + "\1"'),
        
        # REDIRECT_URI = "http://localhost..."
        (r'REDIRECT_URI\s*=\s*["\']https?://(?:localhost|127\.0\.0\.1)(?::\d+)?(/[^"\']*)["\']',
         r'REDIRECT_URI = request.host_url.rstrip("/") + "\1"'),
        
        # "redirect_uri": "http://localhost..."
        (r'["\']redirect_uri["\']\s*:\s*["\']https?://(?:localhost|127\.0\.0\.1)(?::\d+)?(/[^"\']*)["\']',
         r'"redirect_uri": request.host_url.rstrip("/") + "\1"'),
    ]
    
    new_content = content
    changed = False
    for pat, repl in pats:
        if re.search(pat, new_content):
            new_content = re.sub(pat, repl, new_content)
            changed = True
    
    if changed:
        # Check if "from flask import ...request" is there
        if "from flask import" not in new_content:
            new_content = "from flask import request\n" + new_content
        elif "request" not in re.search(r'from flask import ([^\n]+)', new_content).group(1):
            new_content = re.sub(r'from flask import ([^\n]+)', r'from flask import \1, request', new_content)
        
        # Clean up double slashes inside the newly built strings
        # We ensure it's not // between base and path
        # Actually our replacement r'request.host_url.rstrip("/") + "\1"' handles this
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated {filepath}")

def main():
    connectors_dir = "backend/connectors"
    for filename in os.listdir(connectors_dir):
        if filename.endswith(".py"):
            fix_connector_file(os.path.join(connectors_dir, filename))

if __name__ == "__main__":
    main()
