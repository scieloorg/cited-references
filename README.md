# Scripts

## Collect PID

Collects PIDs through the [articlemeta api](https://github.com/scieloorg/articlemetaapi).


## Collect Document

Collects Documents through the [articlemeta api](https://github.com/scieloorg/articlemetaapi). It receives the PIDs collected by the `Collect PIDs` script as data input to obtain the documents.


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
