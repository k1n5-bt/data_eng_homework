import csv

diff = 25 + (9 % 10)
result = []

with open('data/text_4_var_9', mode = "r", encoding="utf-8") as f:
    data = csv.reader(f, delimiter=',')
    avg_sum = 0
    count = 0
    for r in data:
        if int(r[3]) > diff:
            q = [int(r[0])]
            q.extend(r[1:-1])
            result.append(q)
        avg_sum += int(r[4][:-1])
        count += 1
    avg_sum /= count

result = sorted([v for v in result if int(v[-1][:-1]) >= avg_sum], key=lambda x: x[0])

with open('data/text_4_var_9_out', mode = 'w', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter=',')
    for item in result:
        name = ' '.join([item[1], item[2]])
        writer.writerow([item[0]] + [name, *item[2:]])
