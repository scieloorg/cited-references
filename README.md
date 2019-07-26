# Scripts

## Collect PID

Collects PIDs through the [articlemeta api](https://github.com/scieloorg/articlemetaapi). Pass a "date from" as an argument is optional. In case of do not pass a "date from" as an argument the script will consider the current date as "date from".

**How to use**

Pass a "date from" as an argument (optional): 

    `$ ./collect_pid.py 2019-06-01`

or do not pass a "date from" as an argument (current date):

    `$ ./collect_pid.py`

The script will result in a CSV file named `new-pids-from-2019-06-01.csv` (in the former case) wrote in the directory of execution. The date part refers to the argument passed as a parameter.


## Collect Document

Collects Documents through the endpoint [http://articlemeta.scielo.org/api/v1/article](http://articlemeta.scielo.org/api/v1/article). It receives the list of PIDs collected by the `Collect PIDs` script as data input to obtain the documents from the remote database SciELO. _The script collects the documents in an async manner_.

**How to use**

    `$ ./collect_document.py new-pids-from-2019-06-01.csv refSciELO_001`

The script will collect all the documents (pids in the list `new-pids-from-2019-06-01.csv`) from the remote database SciELO and insert them into the local database (`refSciELO_001`).


## Create references database ref_scielo

Creates the references database named ref_scielo. The script receives as data input the name of the documents database. A new database (in the local MongoDB) will be created where each _id is `'_'.join([pid, citation_index_number])` and each value is the citation's content of the documents database. The ref_scielo's collections will be the same as of the documents' database.

**How to use**

    `$ ./create_ref_scielo.py refSciELO_001`


## Update references database ref_scielo

Updates the references database named ref_scielo. The script receives as data input the name of the documents database and the new_pids.txt generated in a previous step (collect_pid). The existing ref_scielo database will be updated with the new content of the documents database.

**How to use**

    `$ ./update_ref_scielo.py refSciELO_001 new-pids-from-2019-06-10.csv ref_scielo`


## Analyze Documents

Analyzes Documents with regard to several attributes. It acts in the documents database.


## Analyze References

Analyzes References with regard to several attributes (e.g., presence or not of a valid DOI, type of reference). It acts in the references database.


## Collect CrossRef Metadata from DOI

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the Document's DOI (Reference's DOI) as data input. The DOI's metadata is saved into the references database (fields `crossref_metadata` and `status`), according to the following status codes:

Status | Description
------ | ------------
1 | DOI were found in CrossRef
-1 | DOI were not found in CrossRef
-2 | Request time out (try again)
Empty | DOI not searched

**How to use**

    `$ ./collect_metadata_from_crossref.py ref_scielo rafael.damaceno@ufabc.edu.br no-status`

The term "no-status" informs the script to collect metadata for all the references without a status code, i.e, those that it does not tried to collect in previous executions. For collecting all references with a status -1, change the term "no-status" to "-1".


## Collect CrossRef Metadata from PID Metadata

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the document's metadata as data input.


## Collect CrossRef Metadata from Reference Metadata

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the references' metadata as data input.


## Create/Update Dictionary Metadata: PID SciELO

Creates (or updates) a dictionary of Document's Metadata (key) to PID SciELO (value). It receives SciELO documents metadata as data input. Returns a dictionary in binary format. The key's components could be configured according to the following:

Description | Example
----------- | -------
First author - first given name | rafael
First author - first given name - first char | r
First author - last surname | damaceno
Publication date | 1995-05-20
Journal title | revista de saude publica
Journal title abbreviated | rev sau pub
Number of the issue | 1
Number of the issue order | 1 
Number of the issue volume | 1
Number of the first page | 10



## Match reference with PID SciELO

Match Documents' references (getting its metadata to generate a key) with the dictionary of Metadata: PID SciELO.


## Utils

This package includes several micro auxiliary scripts. One of them is `count` that counts the number of registers in a database.

### Count

**How to use**

    `$ ./utils/count.py ref_scielo`

where ref_scielo is the database's name.


### Status Check

**How to use**

    `$ ./utils/status_ckeck.py ref_scielo`

where ref_scielo is the database's name.


## String Processor

File `string_processor.py` contains the class StringProcessor, which has four methods. These methods are responsible for normalizing texts, as follows:

1. _remove_accents(text: str)_. This method remove accents.

2. _remove_double_spaces(text: str)_. This method remove double spaces.

3. _alpha_num_space(text: str)_. This method holds in the string only alpha and space characters.

4. _preprocess_name(text: str)_. This method first of all remove the string accents, then it holds in the text only alpha and space characters, and finally it removes double spaces.
