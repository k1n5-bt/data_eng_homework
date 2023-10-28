def main():
    result = {}

    with open('data/text_1_var_9', 'r') as f:
        lines = f.readlines()
        for sep in ['!', '.', ',', '?']:
            lines = [line.strip().replace(sep, ' ') for line in lines]

    for line in lines:
        for w in line.split():
            result[w] =  result[w] + 1 if w in result else 1

    result = dict(sorted(result.items(), reverse=True, key=lambda item: item[1]))

    with open('data/text_1_var_9_out', 'w') as f:
        out = '\n'.join([f'{key}:{value}' for key, value in result.items()])
        f.write(out)


if __name__ == '__main__':
    main()
