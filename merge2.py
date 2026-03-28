import os, re

with open('run_all.py', 'r', encoding='utf-8') as f:
    run_all_content = f.read()
with open('gen_html.py', 'r', encoding='utf-8') as f:
    gen_html_content = f.read()
with open('run_all_console.py', 'r', encoding='utf-8') as f:
    run_all_console_content = f.read()

# remove from run_all import ...
console_logic = re.sub(r'from run_all import \([\s\S]*?\)\n', '', run_all_console_content)

# get core of run_all
run_all_core = run_all_content.split('if __name__ == "__main__":')[0]

# gen_html functions (rename main to gen_html_main)
gen_html_funcs = gen_html_content.replace('def main():', 'def gen_html_main():').split('if __name__ == "__main__":')[0]
# remove imports from gen_html that are redundant:
gen_html_funcs = re.sub(r'import os, json\nfrom datetime import datetime\n', '', gen_html_funcs)
gen_html_funcs = re.sub(r'ROOT = os\.path\.dirname.*?\nINDEX_HTML = os\.path\.join\(ROOT, \'index\.html\'\)', '', gen_html_funcs, flags=re.DOTALL)

# remove second shebang
run_all_core = re.sub(r'#!/usr/bin/env python3\n\"\"\"Market screener.*?\"\"\"\n', '', run_all_core, flags=re.DOTALL)

combined = f'''#!/usr/bin/env python3
\"\"\"Market screener runner  console output + HTML generation. Note: merged with core modules.\"\"\"

# --- MODULES FROM RUN_ALL.PY ---
''' + run_all_core + '''

# --- GEN_HTML.PY LOGIC ---
''' + gen_html_funcs + '''

# --- RUN_ALL_CONSOLE.PY LOGIC ---
''' + console_logic

with open('run_all_console.py', 'w', encoding='utf-8') as f:
    f.write(combined)
