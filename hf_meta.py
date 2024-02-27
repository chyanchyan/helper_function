import re


def get_column_list_from_comments_in_file(filepath, mark):
    rs = open(filepath, 'r', encoding='utf-8').readlines()
    rs = [line.strip() for line in rs]
    rs = [line[len(mark) + 3:] for line in rs if line[:len(mark) + 3] == f'# {mark} ']

    for i, r in enumerate(rs):
        if r == '[delta]':
            rs[i] = rs[i - 1] + '差异'

    return rs


def get_column_list_from_strings_in_function(filepath, function_name, pattern):
    rs = open(filepath, 'r', encoding='utf-8').readlines()
    rs = [line.strip() for line in rs]
    i = 0
    j = 0
    if len(rs) == 0:
        return []
    for i, r in enumerate(rs):
        if r[:4] == 'def ':
            if r[4:].split('(')[0] == function_name:
                for j, rr in enumerate(rs[i + 1:], start=i + 1):
                    if rr[:4] == 'def ':
                        break
                break

    res = []
    rs = rs[i + 1: j]
    for r in rs:
        for i in range(len(r)):
            if r[i] == "'":
                for j in range(len(r[i + 1:])):
                    if r[j] == "'":
                        s = r[i + 1: j]
                        if re.match(pattern, s) is not None:
                            res.append(s)
                            i = j + 1
    res = list(set(res))

    return res
