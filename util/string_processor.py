import re
import unicodedata


class StringProcessor(object):

    parenthesis_pattern = re.compile(r'[-a-zA-ZÀ-ÖØ-öø-ÿ|0-9]*\([-a-zA-ZÀ-ÖØ-öø-ÿ|\s|.|-|0-9]*\)[-a-zA-ZÀ-ÖØ-öø-ÿ|0-9]*', re.UNICODE)

    @staticmethod
    def remove_invalid_chars(text): 
        vchars = [] 
        for t in text: 
            if ord(t) == 11: 
                vchars.append(' ') 
            elif ord(t) >= 32 and ord(t) != 127: 
                vchars.append(t) 
        return ''.join(vchars)

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
    def preprocess_journal_title(text, remove_parenthesis_info=True, remove_invalid_chars=False):
        if remove_invalid_chars:
            text = StringProcessor.remove_invalid_chars(text)

        if remove_parenthesis_info:
            parenthesis_search = re.search(StringProcessor.parenthesis_pattern, text)
            while parenthesis_search is not None:
                text = text[:parenthesis_search.start()] + text[parenthesis_search.end():]
                parenthesis_search = re.search(StringProcessor.parenthesis_pattern, text)
        return StringProcessor.remove_double_spaces(StringProcessor.alpha_num_space(StringProcessor.remove_accents(text), include_arroba=True))
