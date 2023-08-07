# File Recovering

In this folder you can find all the code for treating the collection. All the process is divided into 3 phases:
* simple file recovering
* manual analysis
* bom checking

## Phase 1: Simple File Recovering

The <code>simple_file_recover.py</code> script will try to assign a possible extension to all the files that has an empty one by using the Mime Type reported in the <code>check_datasets.py</code> logs. The only required argument for the script is the path to the folder where all the datasets are stored.

<code> python3 simple_file_recover.py path_datasets_folder </code>

The file that cannot be recovered will be logged in the <code>/logs/simple_file_recover.log</code> file. 

## Phase 2: Manual Analysis

In this phase I have manually analyze all the collection files by starting from the <code>simple_file_recover.py</code> logs and the <code>check_datasets.py</code> logs and I manually check the extensions of the not RDF files and I extract all the archives (and I also check their content). 

## Phase 3: BOM Checking

The <code>bom_checker.py</code> script will check if a RDF file contains a UTF-8 BOM sequence at the beginning. This convention sometimes give problems to the RDFLib parsing phase, so we want to remove it. The only required argument for the script is the path to the folder where all the datasets are stored.

<code> python3 bom_checker.py path_datasets_folder </code>

