import re

with open('run_all_console.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace gen_html.py's load_history with gen_html_load_history
text = text.replace('def load_history():', 'def gen_html_load_history():', 1) # only replace the second occurrence ideally, wait.
