from util.string_processor import StringProcessor
from xylose.scielodocument import Article


class DocumentManager(object):
    PID = 0
    DOCUMENT_TYPE = 1
    FIRST_AUTHOR_GIVEN_NAMES = 2
    FIRST_AUTHOR_SURNAME = 3
    PUBLICATION_DATE = 4
    JOURNAL_TITLE = 5
    JOURNAL_ABBRV = 6
    JOURNAL_ISSN_PPUB = 7
    JOURNAL_ISSN_EPUB = 8
    ISSUE_NUMBER = 9
    ISSUE_ORDER = 10
    ISSUE_VOLUME = 11
    FIRST_PAGE = 12

    @staticmethod
    def get_doc_attrs(document):
        """
        Returns a list of the document's attributes.
        It is useful for creating/updating dicionaries of metadata2pid.
        """
        pid = document.get('_id')
        xydoc = Article(document)
        document_type = xydoc.document_type.lower()
        first_author = xydoc.first_author

        if first_author is None:
            first_author = {}

        if 'given_names' in first_author:
            first_author_given_names = StringProcessor.preprocess_name(first_author.get('given_names', '').lower())
        else:
            first_author_given_names = ''

        if 'surname' in first_author:
            first_author_surname = StringProcessor.preprocess_name(first_author.get('surname', '').lower())
        else:
            first_author_surname = ''

        publication_date = xydoc.publication_date
        journal_title = StringProcessor.preprocess_journal_title(xydoc.journal.title.lower())
        journal_abbrev_title = StringProcessor.preprocess_journal_title(xydoc.journal.abbreviated_title.lower())

        journal_issn_ppub = xydoc.journal.print_issn
        if journal_issn_ppub is None:
            journal_issn_ppub = ''

        journal_issn_epub = xydoc.journal.electronic_issn
        if journal_issn_epub is None:
            journal_issn_epub = ''

        try:
            issue_number = xydoc.issue.number
            issue_order = xydoc.issue.order
            issue_volume = xydoc.issue.volume
        except:
            issue_number = ''
            issue_order = ''
            issue_volume = ''

        if issue_number is None:
            issue_number = ''

        if issue_order is None:
            issue_order = ''

        if issue_volume is None:
            issue_volume = ''

        start_page = xydoc.start_page
        if xydoc.start_page is None:
            start_page = ''

        del xydoc

        return [pid, document_type, first_author_given_names, first_author_surname, publication_date, journal_title,
                journal_abbrev_title, journal_issn_ppub, journal_issn_epub, issue_number, issue_order, issue_volume,
                start_page, document.get('collection')]
