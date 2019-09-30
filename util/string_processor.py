import unicodedata


class StringProcessor(object):

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
    def preprocess_journal_title(text):
        return StringProcessor.remove_double_spaces(StringProcessor.alpha_num_space(StringProcessor.remove_accents(text), include_arroba=True))
