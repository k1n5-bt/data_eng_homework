from json import dump


def safe_to_json(data: dict | list, path: str):
    with open(path, mode='w', encoding='utf-8') as f:
        dump(data, f, ensure_ascii=False)
