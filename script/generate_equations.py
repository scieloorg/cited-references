import argparse
import logging
import statistics
import os

from core.model.file_manager import FileManager


MIN_YEAR_VOL_FREQ = int(os.environ.get('MIN_YEAR_FREQ_VOL', 3))


def calculate_b(x: list, y: list):
    # x times y
    xy = [a * b for a, b in zip(x, y)]

    # summation (x times y)
    sum_xy = sum(xy)

    # (summation x) times (summation y)
    sum_x_sum_y = sum(x) * sum(y)

    # size of x and y
    n = len(x)

    # summation (x squared)
    sum_x2 = sum([pow(xi, 2) for xi in x])

    # summation x
    sum_x = sum(x)

    numerator = (sum_xy - (sum_x_sum_y/n))
    denonimator = (sum_x2 - (pow(sum_x, 2) / n))

    try:
        b = numerator/denonimator
    except ZeroDivisionError:
        return ''
    return b


def calculate_a(b: float, x: list, y: list):
    # y mean
    y_mean = statistics.mean(y)

    # x mean
    x_mean = statistics.mean(x)

    a = y_mean - (b * x_mean)
    return a


def calculate_error(a: float, b: float, x: list, y: list):
    predicted_y_list = [a + (b * xi) for xi in x]

    # summation squared (y - predicted y)
    squared_error = sum([pow(yi - pyi, 2) for yi, pyi in zip(y, predicted_y_list)])

    # summation (y squared)
    sum_y2 = sum([pow(yi, 2) for yi in y])

    # size of x and y
    n = len(x)

    total_squared_error = sum_y2 - (pow(sum(y), 2)/n)

    try:
        r2 = 1 - squared_error/total_squared_error
    except ZeroDivisionError:
        return ''
    return r2


def calculate_sample(a: float, b: float, x_value: int):
    return a + (b * x_value)


def generate_equation(year_volume_str: list):
    els = [vi.split('|') for vi in year_volume_str]
    year_s = []
    volume_s = []

    for e in els:
        year_s.append(int(e[0]))
        volume_s.append(int(e[1]))

    if len(year_s) > 1 and len(volume_s) > 1:
        b = calculate_b(year_s, volume_s)
        if b != '':
            a = calculate_a(b, year_s, volume_s)
            r2 = calculate_error(a, b, year_s, volume_s)
            if r2 != '':
                return '%.6f|%.6f|%.6f' % (a, b, r2)


def remove_less_frequent_data(year_volume):
    '''
    Remove anos e volumes pouco frequentes de uma lista de anos e volumes

    Params
    ------
    year_volume: list
        Lista contendo strings do tipo ano|volume

    Returns
    -------
    list
        Lista contendo strings do tipo ano|volume com frequencia >= MIN_YEAR_VOL_FREQ
    '''
    yv_to_freq = {}
    for yv in year_volume:
        if yv not in yv_to_freq:
            yv_to_freq[yv] = 0
        yv_to_freq[yv] += 1

    return [yv for yv in yv_to_freq if yv_to_freq[yv] >= MIN_YEAR_VOL_FREQ]


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--base_issnl_to_all',
        required=True,
    )

    parser.add_argument(
        '--base_year_volume',
        required=True,
    )

    parser.add_argument(
        '--output_dir',
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

    logging.info(f'Carregando {args.base_issnl_to_all}')
    i2l, i2t = FileManager.load_issnl2all_base(args.base_issnl_to_all)

    logging.info(f'Carregando {args.base_year_volume}')
    issn2yv, title2yv, il2yv = FileManager.extract_slr_data(args.base_year_volume, i2l)

    with open(os.path.join(args.output_dir, 'equations_issn.csv'), 'w') as fout:
        fout.write('|'.join(['ISSN', 'a', 'b', 'r2']) + '\n')

        for issn, v in issn2yv.items():
            v_cleaned = remove_less_frequent_data(v)
            issn_equation = generate_equation(v_cleaned)

            if issn_equation:
                fout.write('|'.join([issn, issn_equation]) + '\n')
    
    with open(os.path.join(args.output_dir, 'equations_title.csv'), 'w') as fout:
        fout.write('|'.join(['TITLE', 'a', 'b', 'r2']) + '\n')
        
        for title, v in title2yv.items():
            v_cleaned = remove_less_frequent_data(v)
            title_equation = generate_equation(v_cleaned)

            if title_equation:
                fout.write('|'.join([title, title_equation]) + '\n')
    
    with open(os.path.join(args.output_dir, 'equations_issnl.csv'), 'w') as fout:
        fout.write('|'.join(['ISSN-L', 'a', 'b', 'r2']) + '\n')
        
        for il, v in il2yv.items():
            v_cleaned = remove_less_frequent_data(v)
            il_equation = generate_equation(v_cleaned)

            if il_equation:
                fout.write('|'.join([il, il_equation]) + '\n')


if __name__ == '__main__':
    main()
