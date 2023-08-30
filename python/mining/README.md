# Collection Mining

In this folder you can find all the code for the ACORDAR collection mining. The mining phase will extract for every dataset the entities, literals, properties and classes present in the dataset files by using different possible types of file parsing. In my experiments I tried three types of parsing: 
* standard parsing
* label parsing v1
* label parsing v2
* stream parsing

by using every time a different combination of parsers. 

Every parser will produce a <code>dataset_content_parsername.json</code> file with 4 different lists: for classes, entities, literals and properties. Every parser will also log in the <code>dataset_metadata.json</code> file which files has mined. 

The developed parsers are:
- <code>AcordarJenaExtractor.java</code>  in the <code> ADE/java</code> directory. To start this parser you have to provide the datasets folder path in the command line arguments.

<code> java AcordarJenaExtractor.java path_to_datasets_folder</code>

- <code>rdflib_extractor.py</code>. To start this parser you have to provide the datasets folder path in the command line arguments and you can also set the resume mechanism with the --resume argument

<code> python3 rdflib_extractor.py path_to_datasets_folder [--resume]</code>

- <code>lightrdf_extractor.py</code>. To start this parser you have to provide the datasets folder path in the command line arguments and you can also set the resume mechanism with the --resume argument

<code> python3 lightrdf_extractor.py path_to_datasets_folder [--resume]</code>

- <code>rdflibhr_extractor.py</code>. To start this parser you have to provide the datasets folder path in the command line arguments and you can also set the resume mechanism with the --resume argument. You also have to provide the parsing version that you want with the version argument (1 or 2).

<code> python3 rdflibhr_extractor.py path_to_datasets_folder version [--resume]</code>

## Standard Parsing

To start the standard parsing you have to run in this order:
* the <code>AcordarJenaExtractor.java</code> 
* the <code>rdflib_extractor.py</code> 
* the <code>lightrdf_extractor.py</code>

The <code>rdflib_extractor.py</code> will try to parse the files that cannot be parsed by the <code>AcordarJenaExtractor.java</code> and the files that cannot be parsed by the <code>rdflib_extractor.py</code> because too big will be parsed by the <code>lightrdf_extractor.py</code>

## Label Parsing v.1

To start the standard parsing you have to run in this order:
* the <code>rdflibhr_extractor.py</code> with version 1
* the <code>lightrdf_extractor.py</code>

## Label Parsing v.2

To start the standard parsing you have to run in this order:
* the <code>rdflibhr_extractor.py</code> with version 2
* the <code>lightrdf_extractor.py</code>
