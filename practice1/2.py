with open('data/text_2_var_9', 'r') as f:
    lines = f.readlines()

with open('data/text_2_var_9_out', 'w') as f:
    for l in lines:
        elems = l.strip().split()
        f.write(f"{sum(map(int, elems))/len(elems)}\n")
