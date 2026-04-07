import os

def bulk_replace(directory, extensions):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if any(file.endswith(ext) for ext in extensions):
                filepath = os.path.join(root, file)
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                new_content = content
                
                # Patterns to replace (order matters: trailing slash first)
                patterns = [
                    ('http://localhost:7860/', '/'),
                    ('http://localhost:4000/', '/'),
                    ('http://localhost:3000/', '/'),
                    ('http://127.0.0.1:7860/', '/'),
                    ('http://127.0.0.1:4000/', '/'),
                    ('http://127.0.0.1:3000/', '/'),
                    ('http://localhost:7860', ''),
                    ('http://localhost:4000', ''),
                    ('http://localhost:3000', ''),
                    ('http://127.0.0.1:7860', ''),
                    ('http://127.0.0.1:4000', ''),
                    ('http://127.0.0.1:3000', ''),
                ]
                
                for old, new in patterns:
                    new_content = new_content.replace(old, new)
                
                # Ensure no double slashes like //path (common if the replacement was empty and path started with /)
                # But be careful not to break http://
                # We'll just fix double slashes at the start of strings or after quotes
                import re
                new_content = re.sub(r'([\'"])/+', r'\1/', new_content)
                new_content = re.sub(r'>/+', r'>/', new_content)
                
                if new_content != content:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    print(f"Updated {filepath}")

if __name__ == "__main__":
    bulk_replace("frontend/templates", [".html"])
    bulk_replace("frontend/static/js", [".js"])
