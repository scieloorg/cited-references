#!/usr/bin/env python3
from pymongo import MongoClient
from xylose.scielodocument import Article

CHARS_FOR_CHECKING = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '!', '‼', '¡', '?', '¿', '«', '»', '(', ')',
                      '[', ']', '{', '}', '‖', '§', '¶', '@', '*', '/', '\\', '%', '‰', '©', '®', '±', '≠', '∗', '≈',
                      '¹', '½', '¼', '²', '³', '¾', '́', '̀', '̆', '̂', '̌', '̊', '̈', '̋', '̃', '̇', '̧', '̨', '̄',
                      '̅', '̣', '͡', 'َ', 'ِ', 'ٕ', ' ', ' ', '_', '֊', '‑', '‐', '‒', '–', '—', '，', ':', '．',
                      '…', '·', '‘', '’', '‚', '‹', '›', '"', '“', '”', '„', '‟', '†', '‡', '•',
                      '′', '־', '׳', '״', '΄', '´', '˜', '^', '¯', '˘', '˙', '¨', '˚', '˝', '¸', '˛', 'ˆ', 'ˇ',
                      'ˋ', '°', '↑', '∂', '∅', '∆', '÷', '×', '<', '=', '=', '>', '¬', '|', '¦', '~', '−', '⁄', '√',
                      '∞', '∫', '⊘', '□', '○', '♂', '¤', '¢', '$', '£', '¥', '€', '〇', 'ᵃ', 'ª', 'Æ', 'æ', 'ǽ', 'ƒ',
                      'º', 'Ø', 'ø', 'Ǿ', 'ǿ', 'Œ', 'œ', '™', 'ʼ', 'α', 'Γ', 'Δ', 'ε', 'θ', 'ϕ', 'φ', 'Φ', 'з', 'З',
                      'И', 'и', 'Ш', 'Щ', 'ы', 'Ь', 'ь', 'Ю', 'Я', 'я', 'Ӏ', 'Լ', 'ء', 'ل', '丫', '亮', '任', '俭', '元',
                      '勇', '原', '吴', '子', '孙', '崔', '希', '徐', '德', '惠', '明', '晓', '李', '杨', '毅', '江', '王',
                      '琪', '箭', '赵', '路', '载', '远', '金', '铭', '陆', '飞', '马', '�', '#'}


def extract_person_name(person: dict) -> str:
    p_surname = person.get('surname', '')
    p_given_names = person.get('given_names', '')
    return ' '.join([p_given_names, p_surname])


def extract_authors(authors_groups: dict) -> list:
    extracted_authors = []

    analytic = authors_groups.get('analytic', {})
    a_person = analytic.get('person', [])
    extracted_authors.extend([extract_person_name(p) for p in a_person])

    return extracted_authors


def count_char(text: str, collection, pid, index_number, authors_for_checking):
    chars = {}
    for c in text:
        if c not in chars:
            chars[c] = 0
        chars[c] += 1

    for c in text:
        if c in CHARS_FOR_CHECKING:
            authors_for_checking.add('\t'.join([collection, pid, index_number, c, text]))

    return chars


def merge_char_freq(d1, d2):
    d12 = d1.copy()
    for k in d2:
        if k in d12:
            d12[k] += d2[k]
        else:
            d12[k] = d2[k]
    return d12


if __name__ == "__main__":

    # Local MongoDB containing SciELO documents
    database = MongoClient()['refSciELO_001']

    for collection in sorted(database.list_collection_names()):

        # dict of chars' frequency
        chars_freq = {}

        # set of author's name
        authors_for_checking = set()

        # set of dates
        dates_for_checking = set()

        c = 0
        number_of_documents = database[collection].count_documents({})

        for doc_json in database[collection].find({}):
            c += 1
            print('\r%s: %d / %d' % (collection, c, number_of_documents), end='')

            art = Article(doc_json)
            pid = art.publisher_id

            if art.citations:
                for cit in art.citations:
                    if cit.authors_groups:
                        cit_authors = extract_authors(cit.authors_groups)

                        if cit_authors:
                            cit_char_freq = [count_char(a, collection, pid, str(cit.index_number), authors_for_checking) for a in cit_authors]

                            for ccf in cit_char_freq:
                                chars_freq = merge_char_freq(chars_freq, ccf)

                    is_invalid_date = False
                    if cit.publication_date:
                        for i in cit.publication_date:
                            if i != '-' and not i.isdigit():
                                dates_for_checking.add('\t'.join([collection, pid, str(cit.index_number), cit.publication_date]))
                                is_invalid_date = True
                                break

                        if not is_invalid_date and len(cit.publication_date) < 4:
                            dates_for_checking.add('\t'.join([collection, pid, str(cit.index_number), cit.publication_date]))

        # Save chars data
        file_results_collection = open(collection + '-chars-freq.csv', 'w')
        for t in sorted(chars_freq, key=lambda x: chars_freq[x], reverse=True):
            file_results_collection.write('\t'.join([t, str(chars_freq[t])]) + '\n')
        file_results_collection.close()

        # Save author data
        print()
        file_results = open(collection + '-suspect-authors.csv', 'w')
        for k in authors_for_checking:
            file_results.write(k + '\n')
        file_results.close()

        # Save publication date data
        print()
        file_results = open(collection + '-suspect-dates.csv', 'w')
        for d in dates_for_checking:
            file_results.write(d + '\n')
        file_results.close()
