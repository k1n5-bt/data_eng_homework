diff = 59
result = []

with open('data/text_3_var_9', 'r') as f:
    lines = f.readlines()

with open('data/text_3_var_9_out', 'w') as f:
    for line in lines:
        ls = line.strip().split(',')
        ls1 = (int(v) if v.isdigit() else (int(ls[i - 1]) + int(ls[i + 1])) / 2 for i, v in enumerate(ls))
        result.extend([f"{i}," if pow(i, 0.5) >= diff else '' for i in ls1] + ['\n'])
    f.writelines(result)