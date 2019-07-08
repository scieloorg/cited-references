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


## Analyze Document

Analyzes Documents with regard to several attributes. It acts in the documents database.


## Create/Update citations database refSciELO

Creates or updates the references database named refSciELO.


## Analyze Reference

Analyzes References with regard to several attributes (e.g., presence or not of a valid DOI, type of reference). It acts in the references database.


## Collect CrossRef Metadata from DOI

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the Document's DOI (Reference's DOI) as data input.


## Collect CrossRef Metadata from PID Metadata

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the document's metadata as data input.


## Collect CrossRef Metadata from Reference Metadata

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the references' metadata as data input.


## Create/Update Dictionary Metadata: PID SciELO

Creates (or updates) a dictionary of Document's Metadata (key) to PID SciELO (value). It receives SciELO documents metadata as data input.


## Match reference with PID SciELO

Match Documents' references (getting its metadata to generate a key) with the dictionary of Metadata: PID SciELO.
