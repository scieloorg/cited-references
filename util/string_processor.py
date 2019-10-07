import re
import unicodedata


class StringProcessor(object):

    parenthesis_pattern = re.compile(r'[-a-zA-ZÀ-ÖØ-öø-ÿ|0-9]*\([-a-zA-ZÀ-ÖØ-öø-ÿ|\s|.|-|0-9]*\)[-a-zA-ZÀ-ÖØ-öø-ÿ|0-9]*', re.UNICODE)

    @staticmethod
    def remove_accents(text):
        return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')

    @staticmethod
    def alpha_num_space(text, include_arroba=False):
        new_str = []
        for character in text:
            if character.isalnum() or character.isspace() or (include_arroba and character == '@'):
                new_str.append(character)
            else:
                new_str.append(' ')
        return ''.join(new_str)

    @staticmethod
    def remove_double_spaces(text):
        while '  ' in text:
            text = text.replace('  ', ' ')
        return text.strip()

    @staticmethod
    def preprocess_name(text):
        return StringProcessor.remove_double_spaces(StringProcessor.alpha_num_space(StringProcessor.remove_accents(text)))

    @staticmethod
    def preprocess_journal_title(text, include_parenthesis_info=False):
        if include_parenthesis_info:
            parenthesis_search = re.search(StringProcessor.parenthesis_pattern, text)
            while parenthesis_search is not None:
                text = text[:parenthesis_search.start()] + text[parenthesis_search.end():]
                parenthesis_search = re.search(StringProcessor.parenthesis_pattern, text)
        return StringProcessor.remove_double_spaces(StringProcessor.alpha_num_space(StringProcessor.remove_accents(text), include_arroba=True))
