#!/usr/bin/env python3
import os
import statistics
import sys
sys.path.append(os.getcwd())

from model.file_manager import FileManager


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
    """
    Compute a, b and r2 for a list of year and volume data.
    :param year_volume_str: A list of year and volume data where each element is in the format YEAR|VOLUME (it uses PIPE as separator)
    :return: a, b and r2 with respect to the list of year and volume data
    """
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


if __name__ == '__main__':
    path_year_volume_base = sys.argv[1]
    path_issnl2all = sys.argv[2]

    i2l, i2t = FileManager.load_issnl2all_base(path_issnl2all)
    issn2yv, title2yv, il2yv = FileManager.extract_slr_data(path_year_volume_base, i2l)

    result_issn_equations = open('equations_issn.csv', 'w')
    result_issn_equations.write('|'.join(['ISSN', 'a', 'b', 'r2']) + '\n')
    for issn, v in issn2yv.items():
        issn_equation = generate_equation(v)
        if issn_equation:
            result_issn_equations.write('|'.join([issn, issn_equation]) + '\n')
    result_issn_equations.close()
    
    result_title_equations = open('equations_title.csv', 'w')
    result_title_equations.write('|'.join(['TITLE', 'a', 'b', 'r2']) + '\n')
    for title, v in title2yv.items():
        title_equation = generate_equation(v)
        if title_equation:
            result_title_equations.write('|'.join([title, title_equation]) + '\n')
    result_title_equations.close()
    
    result_il_equations = open('equations_issnl.csv', 'w')
    result_il_equations.write('|'.join(['ISSN-L', 'a', 'b', 'r2']) + '\n')
    for il, v in il2yv.items():
        il_equation = generate_equation(v)
        if il_equation:
            result_il_equations.write('|'.join([il, il_equation]) + '\n')
    result_title_equations.close()
