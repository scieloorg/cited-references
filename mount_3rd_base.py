import sys


def read_equations(path_file_equations):
    raw_eqs = [e.strip().split('|') for e in open(path_file_equations)][1:]
    eqs = {}
    for r in raw_eqs:
        eqs[r[0]] = (float(r[1]), float(r[2]))
    return eqs


def read_primary_base(path_file_primary_base):
    raw_yvb = [y.strip().split('|') for y in open(path_file_primary_base)][1:]

    il2years = {}
    i2years = {}
    i2l = {}

    for r in raw_yvb:
        issnl = r[0]
        issns = r[1].split('#')

        il2years[issnl] = set()

        issns_start_end = r[14].split('#')

        for ise in [j for j in issns_start_end if j != '']:
            els = ise.split('-')
            issn = els[0]
            start = els[1]
            end = els[2]

            if end == '':
                end = '2019'

            if issn not in i2l:
                i2l[issn] = [issnl]
            else:
                i2l[issn].append(issnl)
                i2l[issn] = list(set(i2l[issnl]))

            il2years[issnl].add((issn, start, end))

            if issn not in i2years:
                i2years[issn] = {(start, end)}
            else:
                i2years[issn].add((start, end))

    return il2years, i2years, i2l


def generate_volumes_issn(issn, ab, start_end_set, issn2issnl):
    ivys = []
    a, b = ab

    issnl = issn2issnl.get(issn, [''])[0]

    for ss in start_end_set:
        if ss[0].isdigit() and ss[1].isdigit():
            start = int(ss[0])
            end = int(ss[1])
            for y in range(start, end + 1):
                predicted_volume = a + (b * y)
                if round(predicted_volume) > 0:
                    ivys.append('|'.join([issnl, issn, str(y), str(predicted_volume), str(round(predicted_volume) - 1), str(round(predicted_volume)), str(round(predicted_volume) + 1)]))
    return ivys


def generate_volumes_issnl(issnl, ab, ises):
    iyvs = []
    a, b = ab
    for ise in ises:
        issn = ise[0]
        start = ise[1]
        end = ise[2]
        if start.isdigit() and end.isdigit():
            start = int(start)
            end = int(end)
            for y in range(start, end + 1):
                predicted_volume = a + (b * y)
                if round(predicted_volume) > 0:
                    iyvs.append('|'.join([issnl, issn, str(y), str(predicted_volume), str(round(predicted_volume) - 1), str(round(predicted_volume)), str(round(predicted_volume) + 1)]))
    return iyvs


def save_base(type: str, version: str, terciary_base_data):
    file_path_base = open('terciary_base_' + type + '_' + version + '.csv', 'w')
    file_path_base.write('|'.join(['ISSN-L', 'ISSN', 'YEAR', 'PREDICTED VOLUME (PV)', 'ROUNDED PV - 1', 'ROUNDED PV', 'ROUNDED PV + 1']) + '\n')
    for iiyv in terciary_base_data:
        file_path_base.write(iiyv + '\n')
    file_path_base.close()


def main():
    path_file_equations_issnl = sys.argv[1]
    path_file_equations_issn = sys.argv[2]
    path_file_primary_base = sys.argv[3]
    print('reading equations issnl')
    eqs_issnl = read_equations(path_file_equations_issnl)

    print('reading equations issn')
    eqs_issn = read_equations(path_file_equations_issn)

    print('reading primary base')
    issnl2year, issn2years, issn2issnl = read_primary_base(path_file_primary_base)

    print('generating volumes from issnl equations')
    terciary_base_issnl = []
    for issnl, ab in eqs_issnl.items():
        issn_start_end_set = issnl2year.get(issnl, ())
        terciary_base_issnl.extend(generate_volumes_issnl(issnl, ab, issn_start_end_set))
    save_base('issnl', 'v0.1', terciary_base_issnl)

    print('generating volumes from issn equations')
    terciary_base_issn = []
    for issn, ab in eqs_issn.items():
        start_end_set = issn2years.get(issn, ())
        terciary_base_issn.extend(generate_volumes_issn(issn, ab, start_end_set, issn2issnl))
    save_base('issn', 'v0.1', terciary_base_issn)


if __name__ == '__main__':
    main()
