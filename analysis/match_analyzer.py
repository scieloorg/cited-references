def count_titles_not_matched(ms, s=50):
    for i in sorted(ms, key=lambda x: int(x[1].strip()), reverse=True)[:s]:
        print(i[1].strip(), i[0], sep='\t')


def count_citations_by_title_with_at_least_n_issnls(ms, n, s=50):
    title2citations = {}
    for i in ms:
        if int(i[1]) >= n:
            title = i[0]
            citations = int(i[2])
            if title not in title2citations:
                title2citations[title] = citations
            else:
                title2citations[title] += int(citations)

    for j in sorted(title2citations, key=lambda x: title2citations[x], reverse=True)[:s]:
        print(title2citations[j], j, sep='\t')


def count_citations_by_title_with_n_issnls(ms, n, s=50):
    title_citations = []
    for i in ms:
        if i[1] == str(n):
            title = i[0]
            citations = int(i[2])
            title_citations.append((title, citations))

    for j in sorted(title_citations, key=lambda x: x[1], reverse=True)[:s]:
        print(j[1], j[0], sep='\t')


def count_issnl_by_citations(ms, s=50):
    issnl2citations = {}
    issnl2title = {}
    for i in ms:
        n = int(i[4].strip())
        if n == 1:
            issnl = i[3]
            if issnl not in issnl2citations:
                issnl2citations[issnl] = 1
            else:
                issnl2citations[issnl] += 1

            title = i[2].strip()
            if issnl not in issnl2title:
                issnl2title[issnl] = {title}
            else:
                issnl2title[issnl].add(title)

    for j in sorted(issnl2citations, key=lambda x: issnl2citations[x], reverse=True)[:s]:
        print(j, str(issnl2citations[j]), '#'.join(sorted(issnl2title[j])), sep='\t')


def count_desambiguations_by_n_col(ms):
    col2n = {}
    for i in ms:
        col = i[0].strip()
        ton = int(i[7].strip())

        if col not in col2n:
            col2n[col] = {ton: 1}
        else:
            if ton not in col2n[col]:
                col2n[col][ton] = 1
            else:
                col2n[col][ton] += 1

    print('\t'.join(['colection', '1', '2', '3', '4', '>= 5']))
    for col in sorted(col2n):
        print(col + '\t', end='')
        ton_sum = 0
        for ton in sorted(col2n[col]):
            if ton < 5:
                print(str(col2n[col][ton]) + '\t', end='')
            else:
                ton_sum += col2n[col][ton]
        print(str(ton_sum))


def count_match_by_n_col(ms):
    col2n = {}
    for i in ms:
        col = i[0].strip()
        issns = i[3].split('#')
        n = int(i[4].strip())
        if len(issns) != n:
            print('ERROR: line %s is not ok' % str(i))
        if col not in col2n:
            col2n[col] = {n: 1}
        else:
            if n not in col2n[col]:
                col2n[col][n] = 1
            else:
                col2n[col][n] += 1

    print('\t'.join(['colection', '1', '2', '3', '4', '>= 5']))
    for ci in sorted(col2n):
        print(ci + '\t', end='')
        ni_sum = 0
        for ni in sorted(col2n[ci]):
            if ni < 5:
                print(str(col2n[ci][ni]) + '\t', end='')
            else:
                ni_sum += col2n[ci][ni]
        print(str(ni_sum))


def read_match_data(path_match_data):
    splitted_lines = []
    fs = open(path_match_data)
    for i in fs:
        spi = i.split('\t')
        splitted_lines.append(spi)
    fs.close()
    return splitted_lines


if __name__ == '__main__':
    FILE_MATCH = '/home/rafael/Working/cited-references/util/matches/1572731217943/matches.tsv'
    FILE_ISSNS_MATCHED = '/home/rafael/Working/cited-references/util/matches/1572731217943/issns_matched.tsv'
    FILE_TITLES_NOT_MATCHED = '/home/rafael/Working/cited-references/util/matches/1572731217943/titles_not_matched.tsv'
    FILE_ALL_TITLES = '/home/rafael/Working/cited-references/util/matches/1572731217943/all_titles.tsv'
    FILE_HOMONYMOUS_DESAMBIGUATED = '/home/rafael/Working/cited-references/util/matches_secondary/1572916410561/homonymous_disambiguated.tsv'

    # matches = read_match_data(FILE_MATCH)
    # count_match_by_n_col(matches)
    # count_issnl_by_citations(matches, 50)

    # issns_matches = read_match_data(FILE_ISSNS_MATCHED)
    # for i in range(2, 5):
    #     print('---- N = %s' % str(i))
    #     count_citations_by_title_with_n_issnls(issns_matches, i)
    #     print('')
    # count_citations_by_title_with_at_least_n_issnls(issns_matches, 5)

    # titles_not_matched = read_match_data(FILE_TITLES_NOT_MATCHED)
    # count_titles_not_matched(titles_not_matched, 50)

    homonymous_fixed = read_match_data(FILE_HOMONYMOUS_DESAMBIGUATED)
    count_desambiguations_by_n_col(homonymous_fixed)
