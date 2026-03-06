import os
import glob

path = r"c:/Users/HP/OneDrive/Desktop/PROJECTS/Segmento_Collector/ui/templates/connectors"

files = glob.glob(os.path.join(path, "*.html"))

old = '''
document.getElementById("formatField")
        .classList.toggle(
            "hidden",
            !(type === "bigquery" || type === "s3" || type === "azure_datalake")
        );
'''

new = '''
const formatField = document.getElementById("formatField");
const iceberg = document.getElementById("format-iceberg");

formatField.classList.toggle(
    "hidden",
    !(type === "bigquery" || type === "s3" || type === "azure_datalake")
);

if(type === "bigquery"){
    if(iceberg) iceberg.style.display="none";
}
else if(type === "s3" || type === "azure_datalake"){
    if(iceberg) iceberg.style.display="flex";
}
'''

count = 0

for f in files:

    with open(f,"r",encoding="utf-8") as file:
        content = file.read()

    if old in content:
        content = content.replace(old,new)

        with open(f,"w",encoding="utf-8") as file:
            file.write(content)

        count += 1

print("Updated:",count)