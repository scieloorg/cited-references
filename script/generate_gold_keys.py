import argparse
import csv
import logging
import os

from scielo_scholarly_data import standardizer


CODES = set()


def _get_gold_key_and_value_list(raw_issns):
    std_issns = sorted(set([k for k in [standardizer.journal_issn(i) for i in raw_issns] if k and len(k) == 9]))
    
    if len(std_issns) == 0:
        return ()

    gold_key = std_issns[0]

    return [(gold_key, i) for i in std_issns]
    

def _add_pair_into_dict(key_value, pair):
    key_value.add(pair)


def _detect_issn_cols(path, delimiter):
    with open(path) as fin:
        csv_reader = csv.DictReader(fin, delimiter=delimiter)

        ikeys = [k for k in csv_reader.fieldnames if 'issn' in k.lower()]
        return ikeys


def _extract_scope_from_filename(filename):
    basename, ext = os.path.splitext(filename)
    return basename


def run(input, key_value, scope, delimiter='\t'):
    delimiter = '|' if scope == 'scielo' else delimiter

    issn_keys = _detect_issn_cols(input, delimiter)

    with open(input) as fin:
        counter = 0 
        for row in csv.DictReader(fin, delimiter=delimiter, quoting=csv.QUOTE_NONE):
            counter += 1
            for pair in _get_gold_key_and_value_list([row[ik] for ik in issn_keys]):
                _add_pair_into_dict(key_value, pair)

    logging.info(f'Foram lidas {counter} linhas')


def _find_better_key(k, value_to_key):
    if k in value_to_key:
        return value_to_key[k]


def get_merged_map(key_value):
    global CODES

    key_to_values = {}
    value_to_key = {}

    for k, v in key_value:
        CODES.add(k)
        CODES.add(v)

        bk = _find_better_key(k, value_to_key) or k
        bv = _find_better_key(v, value_to_key)

        if bk not in key_to_values:
            key_to_values[bk] = set()

        if not bv:
            key_to_values[bk].add(v)
            value_to_key[v] = bk

        else:
            key_to_values[bk].add(v)
            value_to_key[v] = bk

            for vi in key_to_values[bv]:
                logging.warning(f'Valor {vi} migrado para chave {bk}')
                key_to_values[bk].add(vi)
                value_to_key[vi] = bk
            
            if bk != bv:
                logging.warning(f'Removida chave {bv}')
                del key_to_values[bv]

    return key_to_values, value_to_key


def save(data, output):
    with open(output, 'w') as fout:
        for gold_issn in sorted(data):
            values = data[gold_issn]
            fout.write('|'.join([gold_issn, '#'.join(sorted(values))]) + '\n')


def check(data):
    all_values = []
    for v in data.values():
        all_values.extend(list(v))

    all_values = set(all_values)

    logging.info('Verificando se todos os códigos estão presentes')
    for c in CODES:
        if c in data.keys():
            continue

        if c in all_values:
            continue

        logging.error(f'{c} não está no mapa')


def repair(data, key_value, value_to_key):    
    logging.info('Verificando se algum par chave-valor não está contemplado')
    need_repair = True

    while need_repair:
        need_repair = False
        changes = []

        for k, v in key_value:
            kv = value_to_key[k]
            if k not in data[kv]:
                logging.error(f'{k} não foi mesclado com seu par')

            vv = value_to_key[v]
            if v not in data[vv]:
                logging.error(f'{v} não foi mesclado com seu par')

            if vv != kv:
                logging.error(f'{k} e {v} estão em registros diferentes; {kv} e {vv} devem ser mesclados')
                need_repair = True

                changes.append({
                    'key': k, 
                    'value': v,
                    'value_to_key_of_k': kv, 
                    'value_to_key_of_v': vv
                })

        for c in changes:
            gold_key = c['value_to_key_of_k']
            bad_key = c['value_to_key_of_v']

            values_to_migrate = data.get(bad_key, [])

            for v in values_to_migrate:
                logging.info(f'Migrando {v} para {gold_key}')
                data[gold_key].add(v)
                data[gold_key].add(c['key'])
                data[gold_key].add(c['value'])

            try:
                logging.info(f'Removendo chave {bad_key}')
                del data[bad_key]
            except KeyError:
                logging.warning(f'{bad_key} já tinha sido removida')

            for v in values_to_migrate:
                value_to_key[v] = gold_key
          

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--output_file',
        help='Caminho do arquivo de saída do processamento',
    )

    parser.add_argument(
        '--input_dir',
        help='Caminho do diretório com arquivos de entrada do processamento',
    )

    parser.add_argument(
        '--loglevel',
        default=logging.DEBUG,
        help='Escopo de mensagens de log',
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    key_value = set()

    for f in [i for i in os.listdir(args.input_dir) if i.endswith('.csv') and 'issn_map' not in i]:
        logging.info(f'Processando {f}')
        run(os.path.join(args.input_dir, f), key_value, _extract_scope_from_filename(f))

    logging.info('Obtendo mapa de códigos ISSN')
    data, value_to_key = get_merged_map(key_value)

    logging.info(f'Finalizando mesclas de códigos')
    repair(data, key_value, value_to_key)

    logging.info(f'Verificando corretude dos dados')
    check(data)

    logging.info(f'Gravando mapa de códigos ISSN em {args.output_file}')
    save(data, args.output_file)


if __name__ == '__main__':
    main()
