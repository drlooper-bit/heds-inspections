import re
p = 'F:/INS APP/assets/index-BElEZktv.js'
with open(p, 'r', encoding='utf-8') as f:
    t = f.read()
matches = [m.start() for m in re.finditer(r'[^a-zA-Z]K\s*&&', t)]
for idx in matches:
    print(f'Match at {idx}:')
    print(t[idx-50:idx+250])
    print('='*40)
