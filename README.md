# Scripts

**Clone scripts and install dependencies**

    $ git clone git@github.com:scieloorg/cited-references.git
    $ cd cited-references
    $ virtualenv -p python3 .venv
    $ source .venv/bin/activate
    $ pip3 install -r requirements.txt


## Collect PID

Collects PIDs through the [articlemeta api](https://github.com/scieloorg/articlemetaapi). Pass a "date from" as an argument is optional. In case of do not pass a "date from" as an argument the script will consider the current date as "date from".

**How to use**

Pass a "date from" as an argument (optional): 

    $ ./collect_pid.py 2019-06-01

or do not pass a "date from" as an argument (current date):

    $ ./collect_pid.py

The script will result in a CSV file named new-pids-from-2019-06-01.csv (in the former case) wrote in the directory of execution. The date part refers to the argument passed as a parameter.


## Collect Document

Collects Documents through the endpoint [http://articlemeta.scielo.org/api/v1/article](http://articlemeta.scielo.org/api/v1/article). It receives the list of PIDs collected by the Collect PIDs script as data input to obtain the documents from the remote database SciELO. _The script collects the documents in an async manner_.

**How to use**

    $ ./collect_document.py new-pids-from-2019-06-01.csv refSciELO_001

The script will collect all the documents (pids in the list new-pids-from-2019-06-01.csv) from the remote database SciELO and insert them into the local database (refSciELO_001).


## Create references database ref_scielo

Creates the references database named ref_scielo. The script receives as data input the name of the documents database. A new database (in the local MongoDB) will be created where each _id is '_'.join([pid, citation_index_number]) and each value is the citation's content of the documents database. The ref_scielo's collections will be the same as of the documents' database.

**How to use**

    $ ./create_ref_scielo.py refSciELO_001


## Update references database ref_scielo

Updates the references database named ref_scielo. The script receives as data input the name of the documents database and the new_pids.txt generated in a previous step (collect_pid). The existing ref_scielo database will be updated with the new content of the documents database.

**How to use**

    $ ./update_ref_scielo.py refSciELO_001 new-pids-from-2019-06-10.csv ref_scielo


## Analyze Documents

Analyzes Documents with regard to several attributes. It acts in the documents database.


## Analyze References

Analyzes References with regard to several attributes (e.g., presence or not of a valid DOI, type of reference). It acts in the references database.


## Collect CrossRef Metadata from DOI

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the Document's DOI (Reference's DOI) as data input. The DOI's metadata is saved into the references database (fields crossref_metadata and status), according to the following status codes:

Status | Description
------ | ------------
1 | DOI were found in CrossRef
-1 | DOI were not found in CrossRef
-2 | Request time out (try again)
Empty | DOI not searched

**How to use**

    $ ./collect_metadata_from_crossref.py ref_scielo rafael.damaceno@ufabc.edu.br no-status

The term "no-status" informs the script to collect metadata for all the references without a status code, i.e, those that it does not tried to collect in previous executions. For collecting all references with a status -1, change the term "no-status" to "-1" (the same idea is valid wit the "-2" status).


## Collect CrossRef Metadata from PID Metadata

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the document's metadata as data input.


## Collect CrossRef Metadata from Reference Metadata

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the references' metadata as data input.


## Create dictionary Metadata to PID SciELO

Creates dictionaries where each key is a comma separated string of document's metadata (attributes) and the value is the corresponding PID SciELO. It receives SciELO documents database name as data input. Returns two dictionaries in the binary format (readable through "pickle"). There are, until now, two combinations of document's attributes. The possible attributes are as follows:

Code | Description | Example
---- | ----------- | -------
FA | First author - first given name | rafael (NOT USED)
FAC | First author - first given name - first char | r
FALS | First author - last surname | damaceno
PD | Publication date | 1995-05-20
JT | Journal title | revista de saude publica
JTA | Journal title abbreviated | rev sau pub
IN | Number of the issue number | 1 
IV | Number of the issue volume | 1
FP | Number of the first page | 10

The two combinations are as follows:

_Dictionary Major_

    FAC,FALS,PD,JT,IN,IV,FP

_Dictionary Minor_

    FAC,FALS,PD,JT,IN,IV

**How to use**

    $ ./create_metadata2pid.py refSciELO_001


## Update dictionary Metadata to PID SciELO

Updates the dictionaries of Document's Metadata (key) to PID SciELO (value). It receives SciELO documents database name and the list of new pids to be included in the dictionaries. Returns new dictionaries in binary format. 

**How to use**

    $ ./update_metadata2pid.py refSciELO_001 new-pids-from-2019-06-10.csv


## Match reference with PID SciELO

Match Documents' references (getting its metadata to generate a key) with the dictionary of Metadata: PID SciELO.


## Utils

This package includes several micro auxiliary scripts. One of them is count that counts the number of registers in a database.

### Count

**How to use**

    $ ./utils/count.py ref_scielo

where ref_scielo is the database's name.


### Status Check

**How to use**

    $ ./utils/status_ckeck.py ref_scielo

where ref_scielo is the references database's name.


## String Processor

File string_processor.py contains the class StringProcessor, which has four methods. These methods are responsible for normalizing texts, as follows:

1. _remove_accents(text: str)_. This method remove accents.

2. _remove_double_spaces(text: str)_. This method remove double spaces.

3. _alpha_num_space(text: str)_. This method holds in the string only alpha and space characters.

4. _preprocess_name(text: str)_. This method first of all remove the string accents, then it holds in the text only alpha and space characters, and finally it removes double spaces.


## Scrappers

These are the scripts responsible for collecting data from several indexes databases. The purpose is to gain information related to he name and abreviated name of journals

### Latindex

**How to use**

    $ ./scrapper/latindex.py collect # to collect data
    $ ./scrapper/latindex.py collect # to parse data to a csv file

### WebOfScience

**How to use**

    $ ./scrapper/wos.py collect # to collect data
    $ ./scrapper/wos.py collect # to parse data to a csv file
    