import os
import glob
import re

path = r"c:/Users/HP/OneDrive/Desktop/PROJECTS/Segmento_Collector/ui/templates/connectors"

files = glob.glob(os.path.join(path, "*.html"))

# Regex to match the toggle line, capturing the condition inside the !() for preservation
# It handles optional '|| type === "databricks"' and whitespace variations
pattern = r'''
document\.getElementById\s*\(\s*"formatField"\s*\)\s*\.\s*classList\s*\.\s*toggle\s*\(\s*  # Start of toggle
"hidden",\s*  # "hidden",
\s*!\s*\(\s*type === "bigquery" \|\| type === "s3" \|\| type === "azure_datalake"  # Base condition
(\|\| type === "databricks")?\s*  # Optional databricks part (captured in group 1)
\)\s*  # End of !()
\s*\);  # End of toggle call
'''
regex = re.compile(pattern, re.VERBOSE | re.MULTILINE | re.DOTALL)

count = 0

for f in files:
    with open(f, "r", encoding="utf-8") as file:
        content = file.read()

    if regex.search(content):
        # Capture the full condition for preservation
        match = regex.search(content)
        base_condition = 'type === "bigquery" || type === "s3" || type === "azure_datalake"'
        databricks_part = match.group(1) if match.group(1) else ''
        full_condition = f'({base_condition}{databricks_part})'

        # New code snippet, using the captured full_condition
        new_code = f'''
document.getElementById("formatField").classList.toggle("hidden", !({full_condition}));

const icebergBtn = document.getElementById("format-iceberg");
if (type === "bigquery") {{
    if (icebergBtn) {{
        icebergBtn.classList.add("hidden");
        // Fallback to Parquet if Iceberg was selected
        if (document.getElementById("dataFormat").value === "iceberg") {{
            selectFormat("parquet");  // Assumes selectFormat exists; adjust if needed
        }}
    }}
}} else if (type === "s3" || type === "azure_datalake") {{
    if (icebergBtn) {{
        icebergBtn.classList.remove("hidden");
    }}
}}
'''

        # Replace the matched toggle with the new code (insert after, but since it's a single line replacement, we replace the whole match)
        content = regex.sub(new_code.strip(), content)

        with open(f, "w", encoding="utf-8") as file:
            file.write(content)

        count += 1
        print(f"Updated: {os.path.basename(f)}")

print(f"Total Updated: {count}")