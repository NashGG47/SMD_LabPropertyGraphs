import re

input_file = 'dblp.xml'
output_file = 'dblp_clean.xml'

# Expresión regular para entidades como &ouml;, &uuml;, etc.
entity_pattern = re.compile(r'&[a-zA-Z]+;')

with open(input_file, 'r', encoding='utf-8', errors='ignore') as fin, \
     open(output_file, 'w', encoding='utf-8') as fout:
    
    for line in fin:
        cleaned_line = entity_pattern.sub('', line)
        fout.write(cleaned_line)

print("Archivo limpio guardado como:", output_file)

