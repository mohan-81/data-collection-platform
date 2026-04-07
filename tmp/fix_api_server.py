import re
import sys

def fix_api_server(content):
    # Rule 1: OAuth triggers and callbacks in redirect() should be absolute/dynamic
    # redirect("http://localhost:XXXX/path") -> redirect(request.host_url.rstrip("/") + "/path")
    # We match redirect("http://localhost:XXXX/...) or redirect('http://localhost:XXXX/...)
    content = re.sub(
        r'redirect\(\s*["\']https?://(?:localhost|127\.0\.0\.1)(?::\d+)?(/[^"\']*)["\']\s*\)',
        r'redirect(request.host_url.rstrip("/") + "\1")',
        content
    )
    
    # Rule 2: Hardcoded base URLs in variables should be dynamic or empty
    # BASE_URL = "http://localhost:4000" -> BASE_URL = ""
    content = re.sub(
        r'([A-Z_]+_URL)\s*=\s*["\']https?://(?:localhost|127\.0\.0\.1)(?::\d+)?/?["\']',
        r'\1 = ""',
        content
    )
    
    # Rule 3: Specific catch for redirect_uri strings not in redirect() calls
    # "redirect_uri": "http://localhost:4000/path" -> "redirect_uri": request.host_url.rstrip("/") + "/path"
    # This might need request.host_url. Note: request should be imported.
    content = re.sub(
        r'["\']redirect_uri["\']\s*:\s*["\']https?://(?:localhost|127\.0\.0\.1)(?::\d+)?(/[^"\']*)["\']',
        r'"redirect_uri": request.host_url.rstrip("/") + "\1"',
        content
    )
    
    # Rule 4: CORS origins
    content = content.replace('"http://localhost:3000"', '"*"')
    content = content.replace('"http://127.0.0.1:3000"', '"*"')
    content = content.replace("'http://localhost:3000'", '"*"')
    content = content.replace("'http://127.0.0.1:3000'", '"*"')

    # Rule 5: Catch all other localhost strings and make them relative
    # e.g. "http://localhost:3000/path" -> "/path"
    # But ONLY if not already handled by dynamic ones.
    # We'll use a lookbehind or just be careful.
    # Actually, replacing all remaining http://localhost:XXXX with empty string works 
    # if we ensure no double slash.
    content = re.sub(
        r'https?://(?:localhost|127\.0\.0\.1)(?::\d+)?(/[^"\']*)',
        r'\1',
        content
    )
    
    # Clean up any potential double slashes like //auth/me
    content = content.replace('//', '/')
    # Restore protocol slashes if they were broken (unlikely since we match path start /)
    # content = content.replace(':/', '://') 
    
    return content

if __name__ == "__main__":
    filepath = sys.argv[1]
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    new_content = fix_api_server(content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
