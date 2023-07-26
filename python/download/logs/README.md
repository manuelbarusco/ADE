# Download
<hr>

In this folder you can find all the code for the download phase of the ACORDAR collection. 

The following instructions assume that you are running the code in this directory. 

<hr>

## Phase 1: Download

Execute the <code>downloader.py</code> script to download the ACORDAR collection. This script has the following command line arguments:
* the ACORDAR datasets.json file path (required)
* path to the folder where to download all the datasets (required)
* optional argument (start-from) for the resume mechanism. You can stop the  <code>downloader.py</code> whenever you want and for resuming it you can restart the script and specify this option with the last INDEX printed by the downloader in the CLI. 

<code>
python3 downloader.py ACORDAR/Data/datasets.json path_datasets_folder --start-from LAST_INDEX
</code>
</br>
</br>
In the indicated datasets folder the script will create a folder for every dataset, where it will put all the dataset files and the <code>dataset_metadata.json</code> file:
<br>
<br>

<b> Datasets directories structure; </b>

```
datasets
├── dataset-1
│   ├── curso-sf-dump.rdf
│   ├── curso-sf-dump.ttl
│   └── dataset_metadata.json
├── ...
└── dataset-9995
    ├── dataset_metadata.json
    └── rows.rdf
```

<b> Example of datasets_metadata.json file </b>

```
{
  "dataset_id": "13574",
  "title": "DisGeNET",
  "description": "Linked Data version of DisGeNET, which is a Resource Description Framework (RDF) representation of DisGeNET a vast database integrating gene-disease associations from several public expert-curated data sources (UniProt, CTD, MGD, RGD, GAD) and the literature (text-mining derived associations). The current version (2.1) publishes a network of 13172 diseases and 16666 genes linked by 381056 gene-disease associations. Given the large number of gene-disease associations compiled in DisGeNET, a score has been developed in order to rank the associations based on the supporting evidence. The Linked Data version of DisGeNET is an alternative way to access DisGeNET and provides new opportunities for data integration, querying and crossing DisGeNET data with other external RDF datasets. web: http://rdf.disgenet.org/",
  "author": "IBI Group",
  "tags": "animal model;biomedicine;disease;disgenet;format-dc;format-foaf;format-owl;format-rdf;format-rdfs;format-sio;format-skos;format-void;format-xsd;gene;gene-disease;gene-disease associations;gene-disease network;gene-disease ontology;gene-disease text-mining",
  "downloaded_urls": [
    {
      "url": "http://rdf.disgenet.org/download/v2.1.0/void.ttl",
      "file_name": "void.ttl"
    }
  ],
  "failed_download_urls": [
    "http://rdf.disgenet.org/download/v2.1.0/",
    "http://rdf.disgenet.org/gene-disease-association.ttl#DGN7ab3d8cae0c9f1150cb65a985aa8c0a1",
    "http://rdf.disgenet.org/virtuoso/describe/?url\u003dhttp%3A%2F%2Frdf.disgenet.org%2Fgene-disease-association.ttl%23DGN7ab3d8cae0c9f1150cb65a985aa8c0a1",
    "http://www.disgenet.org/ds/DisGeNET/html/images/disgenet-rdf-schema-125.png"
  ],
  "download_info": {
    "downloaded": 1,
    "total_URLS": 5
  },
```

## Phase 2: Download Retry

The <code>downloader.py</code> script will log in the <code>/logs/downloader_errors.log</code> file all the errors encountered in the download phase. You can run the <code>download_retry.py</code> script in order to retry all the error URLs. 

This script has the following command line arguments:
* path to the folder where to download the dataset files (required)
* optional argument (start-from) for the resume mechanism. You can stop the download whenever you want and for resuming it you can restart the script and specify this option with the last INDEX printed in the CLI. 

<code>python3 retry_download.py path_datasets_folder --start-from LAST_INDEX</code>

All the errors returned by the <code>download_retry.py</code> script will be logged in the 
<code>/logs/download_retry_errors.log</code>

## Phase 3: Download Checking 

The <code>check_datasets.py</code> script will clean up the downloaded file names from the URL noise and it will log in the <code>/logs/check_datasets.log</code> file for every dataset which files have not a valid RDF extension. The only required command line argument is the path to the datasets folder. 

<code>python3 check_datasets.py path_datasets_folder</code>

## Extra: Log Analysis

The <code>log_analysis.ipynb</code> notebook contains the code for the analysis of the HTTP error codes and messages returned by the <code>downloader.py</code> script. 