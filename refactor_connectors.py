import os
import re

TEMPLATE_DIR = r"c:\Users\HP\OneDrive\Desktop\PROJECTS\Segmento_Collector\frontend\templates\connectors"

def refactor_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    filename = os.path.basename(filepath)
    source = filename.replace('.html', '')

    # 1. Extract field IDs from the credentialsForm div
    form_match = re.search(r'id="credentialsForm".*?>(.*?)</div>', content, re.DOTALL)
    field_ids = []
    if form_match:
        form_content = form_match.group(1)
        field_ids = re.findall(r'id="([^"]+)"', form_content)
        # Exclude known buttons or non-input IDs
        field_ids = [fid for fid in field_ids if fid not in ['saveCredsBtn', 'step1-circle', 'step2-circle', 'step3-circle']]

    # 2. Find the script block to replace
    # Most templates have a script block starting with <script> and ending before </section> or </body>
    # specifically, they contain function check<Source>Status
    script_pattern = re.compile(r'<script>.*?(function check' + source.capitalize() + r'Status.*?)</script>', re.DOTALL)
    
    # If the above fails, try a more generic one
    if not script_pattern.search(content):
        script_pattern = re.compile(r'<script>.*?(async function check' + source.capitalize() + r'Status.*?)</script>', re.DOTALL)

    if not script_pattern.search(content):
        print(f"Skipping {filename}: Could not find expected script block.")
        return

    # 3. Create the new script block
    field_ids_str = ", ".join([f"'{fid}'" for fid in field_ids])
    new_script = f"""<script>
  window.connectorManager = new ConnectorManager('{source}', {{
    fieldIds: [{field_ids_str}]
  }});
  window.connectorManager.init();

  // Compatibility aliases for existing inline onclick handlers
  function saveCredentials() {{ window.connectorManager.saveCredentials(); }}
  function runSync() {{ window.connectorManager.runSync(); }}
  function saveJob() {{ /* Handled separately if needed */ }}
  function disconnect{source.capitalize()}() {{ window.connectorManager.disconnect(); }}
  function recoverSync() {{ window.connectorManager.recoverSync(); }}
</script>"""

    # 4. Replace and write back
    new_content = script_pattern.sub(new_script, content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"Refactored {filename} with fieldIds: {field_ids}")

if __name__ == "__main__":
    for filename in os.listdir(TEMPLATE_DIR):
        if filename.endswith(".html"):
            refactor_file(os.path.join(TEMPLATE_DIR, filename))
