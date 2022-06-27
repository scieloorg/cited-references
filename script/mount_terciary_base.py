import argparse
import datetime
import logging
import os


MIN_R2_SCORE = float(os.environ.get('MIN_R2_SCORE', 0.70))


def read_equations(path_file_equations):
    raw_eqs = [e.strip().split('|') for e in open(path_file_equations)][1:]
    eqs = {}

    for r in raw_eqs:
        if not float(r[3]) >= MIN_R2_SCORE:
            logging.warning(f'{r} foi ignorada pois R² ({float(r[3])}) < {MIN_R2_SCORE}')
            continue

        eqs[r[0]] = (float(r[1]), float(r[2]))

    return eqs


def read_primary_base(path_file_primary_base):
    raw_yvb = [y.strip().split('|') for y in open(path_file_primary_base)][1:]

    il2years = {}
    i2years = {}
    i2l = {}
    il2titles = {}

    for r in raw_yvb:
        issnl = r[0]
        issns = r[3].split('#')
        titles = r[4].split('#')

        il2titles[issnl] = titles
        for i in issns:
            if i not in il2titles:
                il2titles[i] = titles
            else:
                il2titles[i].extend(titles)
                il2titles[i] = list(set(il2titles[i]))

        il2years[issnl] = set()

        issns_start_end = r[16].split('#')

        for ise in [j for j in issns_start_end if j != '']:
            els = ise.split('-')
            issn = '-'.join([els[0], els[1]])
            start = els[2]
            end = els[3]

            if end == '':
                end = (datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y')

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

    return il2years, i2years, i2l, il2titles


def generate_volumes_issn(issn, ab, start_end_set, i2t):
    ivys = []
    a, b = ab

    titles = i2t.get(issn, [])

    for t in titles:
        for ss in start_end_set:
            if ss[0].isdigit() and ss[1].isdigit():

                start = int(ss[0])
                end = int(ss[1])

                for y in range(start, end + 1):
                    predicted_volume = a + (b * y)
                    
                    for x in range(-1, 2):
                        if predicted_volume + x > 0:
                            ivys.append('|'.join([
                                issn, 
                                t, 
                                str(y), 
                                str(predicted_volume + x)]))
    return ivys


def generate_volumes_issnl(issnl, ab, ises, i2t):
    outs = set()
    a, b = ab

    for ise in ises:
        issn = ise[0]
        start = ise[1]
        end = ise[2]

        titles = i2t.get(issn)
        
        if start.isdigit() and end.isdigit():
            start = int(start)
            end = int(end)

            for y in range(start, end + 1):
                predicted_volume = a + (b * y)

                for x in range(-1, 2):
                    pvol = int(predicted_volume + x)
                    if pvol > 0:
                        for t in titles:
                            outs.add('|'.join([
                                issnl, 
                                t, 
                                str(y), 
                                str(pvol)])
                            )
    return outs


def save_base(output_path, data):
    with open(output_path, 'w') as fout:
        fout.write('|'.join(['ISSN-L', 'ISSN', 'YEAR', 'VOLUME']) + '\n')

        for iiyv in data:
            fout.write(iiyv + '\n')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--base_issnl_to_all',
        required=True,
    )

    parser.add_argument(
        '--equations_issnl',
        required=True,
    )

    parser.add_argument(
        '--output_file',
        required=True,
    )

    parser.add_argument(
        '--loglevel',
        default=logging.INFO,
        help='Escopo de mensagens de log'
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info(f'Lendo {args.equations_issnl}')
    eqs_issnl = read_equations(args.equations_issnl)

    logging.info(f'Lendo {args.base_issnl_to_all}')
    issnl2year, issn2years, issn2issnl, issnl2titles = read_primary_base(args.base_issnl_to_all)

    logging.info(f'Gerando base ano-volume artificial')
    terciary_base_issnl = []
    for issnl, ab in eqs_issnl.items():
        issn_start_end_set = issnl2year.get(issnl, ())
        terciary_base_issnl.extend(generate_volumes_issnl(issnl, ab, issn_start_end_set, issnl2titles))

    save_base(args.output_file, terciary_base_issnl)


if __name__ == '__main__':
    main()
