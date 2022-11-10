# Scripts

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Scripts
**1. Collect PID code**

Collect PIDs through the [articlemeta api](https://github.com/scieloorg/articlemetaapi). Passing the argument "date_from" is optional. In case of do not passing this argument, the script will use the current date as "date from".

```bash
# With a date
./collect_pid.py \
    2019-06-01

# Without date
./collect_pid.py
```

The script will generate a CSV file named `new-pids-from-2019-06-01.csv` (in the former case) in the local directory. The name's date part refers to the argument passed as parameter.



**2. Collect document metadata**

Collect documents metadata through the endpoint [http://articlemeta.scielo.org/api/v1/article](http://articlemeta.scielo.org/api/v1/article). It receives a list of PIDs collected by the `Collect PID` script as data input to obtain the documents from the remote database SciELO. The script collects the documents in an async manner.

```bash
./collect_document.py \
    new-pids-from-2019-06-01.csv \
    <MONGODB_STR_COLLECTION>
```

The script will collect all the documents (pids in the list new-pids-from-2019-06-01.csv) from the remote database SciELO and insert them into the a local MongoDB database.


**3. Create a references database called ref_scielo**

Create the references database named ref_scielo. The script receives as data input the name of the documents database. A new database (in the local MongoDB) will be created where each \_id is '\_'.join([pid, citation_index_number]) and each value is the citation's content of the documents database. The ref_scielo's collections will be the same as of the documents' database.

```bash
./create_ref_scielo.py ref_scielo
```


**4. Update the references database ref_scielo**

Update the references database named ref_scielo. The script receives as data input the name of the documents database and the new_pids.txt generated in a previous step (collect_pid). The existing ref_scielo database will be updated with the new content of the documents database.

```bash
./update_ref_scielo.py ref_scielo new-pids-from-2019-06-10.csv ref_scielo
```

**5. Analyze Documents**

Analyze Documents with regard to several attributes. It acts in the documents database.

**6. Analyze References**

Analyze References with regard to several attributes (e.g., presence or not of a valid DOI, type of reference). It acts in the references database.


**7. Collect CrossRef Metadata from DOI**

Collect metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the Document's DOI (Reference's DOI) as data input. The DOI's metadata is saved into the references database (fields crossref_metadata and status), according to the following status codes:

Status | Description
------ | ------------
1 | DOI were found in CrossRef
-1 | DOI were not found in CrossRef
-2 | Request time out (try again)
Empty | DOI not searched

```bash
./collect_metadata_from_crossref.py ref_scielo rafael.damaceno@ufabc.edu.br no-status
```

The term "no-status" informs the script to collect metadata for all the references without a status code, i.e, those that it does not tried to collect in previous executions. For collecting all references with a status -1, change the term "no-status" to "-1" (the same idea is valid wit the "-2" status).


**8. Collect CrossRef Metadata from PID Metadata**

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the document's metadata as data input.


**9. Collect CrossRef Metadata from Reference Metadata**

Collects metadata from the [crossref rest-api](https://www.crossref.org/services/metadata-delivery/rest-api/). It receives the references' metadata as data input.


**10. Create dictionary Metadata to PID SciELO**

Create dictionaries where each key is a comma separated string of document's metadata (attributes) and the value is the corresponding PID SciELO. It receives SciELO documents database name as data input. Returns two dictionaries in the binary format (readable through "pickle"). There are, until now, two combinations of document's attributes. The possible attributes are as follows:

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

```bash
./create_metadata2pid.py ref_scielo
```


**11. Update dictionary Metadata to PID SciELO**

Update the dictionaries of Document's Metadata (key) to PID SciELO (value). It receives SciELO documents database name and the list of new pids to be included in the dictionaries. Returns new dictionaries in binary format. 


```bash
./update_metadata2pid.py ref_scielo new-pids-from-2019-06-10.csv
```


**12. Match reference with PID SciELO**

Match Documents' references (getting its metadata to generate a key) with the dictionary of Metadata: PID SciELO.


## Utils

This package includes several micro auxiliary scripts. One of them is count that counts the number of registers in a database.

### Count

```bash
./utils/count.py ref_scielo
```

where ref_scielo is the database's name.


### Status Check

```bash
./utils/status_ckeck.py ref_scielo
```

where ref_scielo is the references database's name.


## String Processor

File string_processor.py contains the class StringProcessor, which has four methods. These methods are responsible for normalizing texts, as follows:

1. _remove_accents(text: str)_. This method remove accents.

2. _remove_double_spaces(text: str)_. This method remove double spaces.

3. _alpha_num_space(text: str)_. This method holds in the string only alpha and space characters.

4. _preprocess_name(text: str)_. This method first of all remove the string accents, then it holds in the text only alpha and space characters, and finally it removes double spaces.


## Scrappers

These are the scripts responsible for collecting data from several indexes databases. The main purpose is to obtain information related to the name and abreviated name of journals. Another object is to obtain information with regard to the values of year and volume of journals. 

### Latindex

**How to use**

```bash
# to collect data
./scrapper/latindex.py collect

# to parse data to a csv file
./scrapper/latindex.py parse 
```

### Locator Plus

```bash
# to collect data related to NLM names inse FILE_NLM_TITLES
./scrapper/locator_plus.py collect FILE_NLM_TITLES 

# to parse html files inside DIR_HTMLS folder to a comma-separated-value file
./scrapper/locator_plus.py parse DIR_HTMLS 
```


### Web of Science
```bash
# to collect data
./scrapper/wos.py collect

# to parse data to a csv file
,/scrapper/wos.py parse
```

### Ulrich
    
```bash
# to collect data in html format
./scrapper/ulrich.py collect

# to parse data to a csv file 
./scrapper/ulrich.py parse 
```


## Cleaner-Filter-Portal

Clean and filter the CSV file provided by issn-scrapper. Removes invalid rows and ignores unnecessary columns.

```bash
# to remove invalid rows and useless columns from the file data.csv
./cleaner/portal.py data.csv 
```
    
which will returns a file named data_filtered.csv
   
    
## Merge indexes base

Merge all CSV files (from the indexes) into one CSV file where each line has one key (ISSN-L) and several columns (ISSN, TITLE, ABBREVIATED-TITLE, EXTRA-TITLE)

```bash
# to merge all CSVs inside folder indexes
./merge_indexes.py indexes/
```
