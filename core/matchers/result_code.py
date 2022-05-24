SUCCESS_EXACT_MATCH = 0                     # Success: exact match occurred through title-issnl correction base
SUCCESS_EXACT_MATCH_YEAR_VOL = 1            # Success: exact match occurred with more than one ISSN and it was possible to decide which one is the correct through year-volume correction base
SUCCESS_EXACT_MATCH_YEAR_VOL_INF = 2        # Success: exact match occurred with more than one ISSN and it was possible to decide which one is the correct through year-volume-inferred correction base
SUCCESS_EXACT_MATCH_YEAR_VOL_ART = 3        # Success: exact match occurred with more than one ISSN and it was possible to decide which one is the correct through artifitial year-volume correction base
SUCCESS_EXACT_MATCH_YEAR_VOL_INF_ART = 4    # Success: exact match occurred with more than one ISSN and it was possible to decide which one is the correct through artifitial year-volume-inferred correction base

SUCCESS_FUZZY_MATCH_YEAR_VOL = 11           # Success: fuzzy match occurred and was validated through year-volume correction base
SUCCESS_FUZZY_MATCH_YEAR_VOL_INF = 12       # Success: fuzzy match occurred and was validated through year-volume-inferred correction base
SUCCESS_FUZZY_MATCH_YEAR_VOL_ART = 13       # Success: fuzzy match occurred and was validated through artifitial year-volume correction base
SUCCESS_FUZZY_MATCH_YEAR_VOL_INF_ART = 14   # Success: fuzzy match occurred and was validated through artifitial year-volume-inferred correction base

ERROR_EXACT_MATCH_YEAR_VOL = 519            # Error: exact match occurred with more than one ISSN, but it was not possible to decide which one is the correct through year-colume correction base
ERROR_EXACT_MATCH_YEAR_VOL_INF = 529        # Error: exact match occurred with more than one ISSN, but it was not possible to decide which one is the correct through year-colume-inferred correction base
ERROR_EXACT_MATCH_YEAR_VOL_ART = 539        # Error: exact match occurred with more than one ISSN, but it was not possible to decide which one is the correct through artifitial year-colume correction base
ERROR_EXACT_MATCH_YEAR_VOL_INF_ART = 549    # Error: exact match occurred with more than one ISSN, but it was not possible to decide which one is the correct through artifitial year-colume-inferred correction base

ERROR_FUZZY_MATCH_YEAR_VOL = 619            # Error: fuzzy match occurred with more than one ISSN, but it was not possible to decide which one is the correct through year-colume correction base
ERROR_FUZZY_MATCH_YEAR_VOL_INF = 629        # Error: fuzzy match occurred with more than one ISSN, but it was not possible to decide which one is the correct through year-colume-inferred correction base
ERROR_FUZZY_MATCH_YEAR_VOL_ART = 639        # Error: fuzzy match occurred with more than one ISSN, but it was not possible to decide which one is the correct through artifitial year-colume correction base
ERROR_FUZZY_MATCH_YEAR_VOL_INF_ART = 649    # Error: fuzzy match occurred with more than one ISSN, but it was not possible to decide which one is the correct through artifitial year-colume-inferred correction base

ERROR_EXACT_MATCH_UNDECIDABLE = 500         # Error: exact match occurred with more than one ISSN, but it was not possible to decide which one is the correct
ERROR_FUZZY_MATCH_UNDECIDABLE = 600         # Error: fuzzy match occurred but was not validated

ERROR_JOURNAL_TITLE_NOT_FOUND = 70          # Error: exact and fuzzy matches did not occur - journal title was not found in the correction bases

ERROR_EXACT_MATCH_INVALID_YEAR = 80         # Error: exact match occurred with more than one ISSN, but it was not possible to decide which one is the correct - cited year is empty or invalid
ERROR_FUZZY_MATCH_INVALID_YEAR = 81         # Error: it was not possible to conduct fuzzy match due to insuficient data - cited year is empty or invalid
ERROR_JOURNAL_TITLE_IS_EMPTY = 82           # Error: it was not possible to match - journal title is empty

NOT_CONDUCTED_MATCH_DOI_EXISTS = 90         # Not conducted: matching was not conducted - get crossref metadata informing the existing doi
NOT_CONDUCTED_MATCH_FORCED_BY_USER = 91     # Not conducted: fuzzy matching was not coducted due to parameter indication - inform the parameter use_fuzzy to activate this method
