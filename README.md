# ADE

ADE (Acordar Download and Extraction) repository contains the code (in Java and Python) for the download and parsing phases of the ACORDAR collection. These are the first steps for a more complete reproducibility study based on the [ACORDAR paper](https://dome40.eu/sites/default/files/2022-11/ACORDAR%20A%20Test%20Collection%20for%20Ad%20Hoc%20Content-Based%20%28RDF%29%20Dataset%20Retrieval.pdf)

The execution pipeline is the follwoing:
1. Collection Download by using the code in the <code>/python/download</code> directory
2. File recovering by using the code in the <code>/python/file_recover</code> directory
3. Dataset parsing by using the code in the <code>/java</code> and <code>python/mining</code> directory 
4. Statistics retrieving by using the Jupyter Notebook in the <code>/python/statistics</code> directory

Inside the <code>/java</code> and <code>/python</code> folders you can find other README.md files with all the instructions for running the code. 

Before executing all the code take a look to the [ACORDAR repository](https://github.com/nju-websoft/ACORDAR) and clone it. You will need the <code>datasets.json</code> that contains the list of all the details of the collection datasets. 

