#!/usr/bin/env python3

FILE_ISSNL2YEARS = '/home/rafael/Temp/scielo/bases/issnl2years_v0.3.csv'
FILE_MATCHES = '/home/rafael/Temp/scielo/bases/matches.tsv'
FILE_BASE_TITLES = '/home/rafael/Temp/scielo/bases/base_titulo2issnl_v0.4.csv'


def diff_years(years):
    results = []
    years = sorted(years)
    for i in range(len(years) - 1):
        tmp_r = years[i + 1] - years[0]
        results.append(tmp_r)
    if len(results) == 1:
        return results[0]
    else:
        return results


def is_valid_year(year: str):
    if len(year) == 4:
        if year.isdigit():
            if int(year) < 2020:
                return True
    return False


if __name__ == '__main__':
    # reads issnl2years file
    tmp_issnl2years = [l for l in open(FILE_ISSNL2YEARS)]
    tmp_issnl2years.pop(0)
    issnl2start = {}

    # mounts dict issnl2years
    for r in tmp_issnl2years[1:]:
        rels = r.split('\t')
        if len(rels[1]) >= 4:
            issnl2start[rels[0].replace('-', '')] = rels[1][:4]

    # reads base title file
    tmp_base = [l for l in open(FILE_BASE_TITLES)]
    tmp_base.pop(0)

    # mount vector of two, three and four issnls (with a common title)
    dup2 = []
    dup3 = []
    dup4 = []
    dup5ormore = []

    for r in tmp_base:
        rels = r.split('\t')
        issnls = rels[1].split('#')
        if len(issnls) > 1:
            if len(issnls) == 2:
                dup2.append(rels)
            elif len(issnls) == 3:
                dup3.append(rels)
            elif len(issnls) == 4:
                dup4.append(rels)
            else:
                dup5ormore.append(rels)

    # for d in dup2:
    #     dels = d[1].split('#')
    #     tmp_y1 = issnl2start.get(dels[0], '')
    #     tmp_y2 = issnl2start.get(dels[1], '')
    #     if is_valid_year(tmp_y1) and is_valid_year(tmp_y2):
    #         tmp_years = [int(tmp_y1), int(tmp_y2)]
    #         dyears = diff_years(tmp_years)
    #         d.append(dyears)
    #     else:
    #         d.append(-1)

    # for i in sorted(dup2, key=lambda x: x[-1], reverse=True):
    #     print('\t'.join([str(ii).strip() for ii in i]))

    # for d in dup3:
    #     dels = d[1].split('#')
    #     tmp_y1 = issnl2start.get(dels[0], '')
    #     tmp_y2 = issnl2start.get(dels[1], '')
    #     tmp_y3 = issnl2start.get(dels[2], '')
    #     if is_valid_year(tmp_y1) and is_valid_year(tmp_y2) and is_valid_year(tmp_y3):
    #         tmp_years = [int(tmp_y1), int(tmp_y2), int(tmp_y3)]
    #         dyears = diff_years(tmp_years)
    #         d.extend(dyears)
    #     else:
    #         d.extend([-1, -1])
    #
    # for i in sorted(sorted(dup3, key=lambda x: x[-2], reverse=True), key=lambda y: y[-1], reverse=True):
    #     print('\t'.join([str(ii).strip() for ii in i]))

    for d in dup4:
        dels = d[1].split('#')
        tmp_y1 = issnl2start.get(dels[0], '')
        tmp_y2 = issnl2start.get(dels[1], '')
        tmp_y3 = issnl2start.get(dels[2], '')
        tmp_y4 = issnl2start.get(dels[3], '')
        if is_valid_year(tmp_y1) and is_valid_year(tmp_y2) and is_valid_year(tmp_y3) and is_valid_year(tmp_y4):
            tmp_years = [int(tmp_y1), int(tmp_y2), int(tmp_y3), int(tmp_y4)]
            dyears = diff_years(tmp_years)
            d.extend(dyears)
        else:
            d.extend([-1, -1, -1])

    for i in sorted(sorted(sorted(dup4, key=lambda x: x[-3], reverse=True), key=lambda y: y[-2], reverse=True), key=lambda z: z[-1], reverse=True):
        print('\t'.join([str(ii).strip() for ii in i]))
