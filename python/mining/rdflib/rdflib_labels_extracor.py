"""
Extracts the ACORDAR baseline needed data from all the datasets files with valid suffixes
The purpose of this extractor is to extract a human readable version of the triples elements
by using the rdfs:label, rdfs:comment properties and other related properties
"""

import pathlib
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF
import json
import os
import logging
import re 
import argparse
import logging
from tqdm import tqdm


RDF_SUFFIXES = [".rdf", ".rdfs", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml", ".ntriples", ".nq", ".trig", ".trix"]

#limit size of 100MB for parsing a file with RDFLib
FILE_LIMIT_SIZE = 100

"""
@param: uri,, string with the uri
@return last part of the uri, that contains the name
"""
def getNameFromUri(uri:str) -> str:
    split = re.split("[/#]", uri)
    return split[-1]

def getLiterals(graph) -> list:
    q = """
    SELECT ?literal { 
        ?s ?p ?literal 
        FILTER isLiteral(?literal)
    }
    """
    match = graph.query(q)

    literals = list()

    for item in match:
        literals.append(str(item[0]))

    return literals

def getClassesAndEntitiesv2(graph) -> dict:
    q = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?s ?hds ?c ?hdc
    WHERE {
        ?s a ?c .

        FILTER(!isBlank(?s)) .
        
        OPTIONAL { 
            ?s ?p ?hds .
            FILTER isLiteral(?hds) .
            FILTER(REGEX(str(?p), "(name|description|label|title)", "i"))
        }
        
        OPTIONAL {
            ?c ?pc ?hdc .
            FILTER isLiteral(?hdc) .
            FILTER(REGEX(str(?pc), "(name|description|label|title)", "i"))
        }
    }
    """
    match = graph.query(q)

    classes = list()
    entities = list()

    #check the availability of labels and descriptions
    for item in match:
        if item[1] is not None:
            entities.append(item[1])
        else: 
            entities.append(item[0])

    
        if item[3] is not None:
            classes.append(item[3])
        else:
            classes.append(getNameFromUri(item[2]))
        
    return classes, entities    

def getPropertiesv2(graph) -> list:
    q = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?p ?hdp  
    WHERE { 
        ?s ?p ?o .
        
        OPTIONAL {
            ?p ?lp ?hdp .
            FILTER isLiteral(?hdp) .
            FILTER(REGEX(str(?lp), "(name|description|label|title)", "i"))
        }
            
    }
    """
    match = graph.query(q)

    properties = list()

    #check the availability of labels and descriptions
    for item in match:
        if item[1] is not None:
            properties.append(item[1])
        else:
            properties.append(getNameFromUri(item[0]))

    return properties

def getClassesAndEntitiesv1(graph) -> dict:
    q = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?s ?ls ?ds ?c ?lc ?dc
    WHERE {
        ?s a ?c
        
        #retrieve a possible label or description
        #for the class or the entity
        OPTIONAL{
            ?c rdfs:label ?lc 
        }
        OPTIONAL{
            ?c rdfs:description ?dc
        }
        
        OPTIONAL{
            ?s rdfs:label ?ls . 
        }
        OPTIONAL{
            ?s rdfs:description ?ds
        }
    }
    """
    match = graph.query(q)

    classes = list()
    entities = list()

    for item in match:
        if item[1] is not None:
            entities.append(item[1])
        elif item[2] is not None:
            entities.append(item[2])
        else:
            entities.append(item[0])


        if item[4] is not None:
            classes.append(item[4])
        elif item[5] is not None:
            classes.append(item[5])
        else:
            classes.append(getNameFromUri(item[3]))


    return classes, entities    

def getPropertiesv1(graph) -> list:
    q = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?p ?l ?d  
    WHERE { 
        ?s ?p ?o .
        OPTIONAL {
            ?p rdfs:label ?l
        }
        OPTIONAL {
            ?p rdfs:description ?d
        }
    }
    """
    match = graph.query(q)

    properties = list()

    for item in match:
        if item[1] is not None:
            properties.append(item[1])
        elif item[2] is not None:
            properties.append(item[2])
        else:
            properties.append(getNameFromUri(item[0]))

    return properties

'''
@param: dataset, name of the dataset
@param: file, object of type DirEntry
@param: dataset_content, dictionary with the dataset content already mined
@param: f_log, miner error log file
@return True if the file is mined, else False
'''
def mineFile(dataset:str, file:object, dataset_content:dict, f_log:object, version:int) -> bool: 
    g = Graph()

    if (os.path.getsize(file.path) / (1024 ** 2)) < FILE_LIMIT_SIZE: 
        try: 
            g.parse(file.path)

            #extract all the info

            classes, entities, properties = None

            if version == 1:
                classes, entities = getClassesAndEntitiesv1(g)
                properties = getPropertiesv1(g)
            
            if version == 2:
                classes, entities = getClassesAndEntitiesv2(g)
                properties = getPropertiesv2(g)

            literals = getLiterals(g)

            dataset_content["classes"].extend(classes)
            dataset_content["entities"].extend(entities)
            dataset_content["literals"].extend(literals)
            dataset_content["properties"].extend(properties)

            #free memory
            del(g)
            del(classes)
            del(entities)
            del(literals)
            del(properties)

        except rdflib.exceptions.ParserError as e:  
            error = str(e).strip("\n")
            f_log.write("Dataset: "+dataset+"\nFile: "+file.name+"\nError: "+error+"\n")
            return False
        except Exception as e :
            error = str(e).strip("\n")
            f_log.write("Dataset: "+dataset+"\nFile: "+file.name+"\nError: "+error+"\n")
            return False
    else :
        f_log.write("Dataset: "+dataset+"\nFile: "+file.name+"\nError: Bigger than "+str(FILE_LIMIT_SIZE)+"\n")
        return False
    
    return True

'''
@param: dataset, object of type DirEntry   
@param: f_log, error log file
@param: resume, boolean used for resume mechanism
@param: version, version of the parsing (1: labels v1, 2: labels v2)
'''
def mineDataset(dataset:object, f_log:object, resume:bool, version:int):

    #open the dataset metadata file
    dataset_metadata_file = open(dataset.path+"/dataset_metadata.json", "r", encoding="utf-8")
    dataset_metadata = json.load(dataset_metadata_file, strict = False)
    dataset_metadata_file.close()

    #checking for the resume mechanism
    if "mined_rdflibhr" in dataset_metadata.keys():
        if dataset_metadata["mined_rdflibhr"] and resume:
            return 

    dataset_content = dict()

    dataset_content["classes"] = list()
    dataset_content["entities"] = list()
    dataset_content["literals"] = list()
    dataset_content["properties"] = list()

    #print("Mining dataset: "+dataset.name)

    mined_files = list()

    #check for the files in the directory and consider them in order 
    #of importance based on their dimension and by skipping same
    #versions but with different extensions of the same file

    files_group = dict()
    for file in os.scandir(dataset.path):
        file_extension = pathlib.Path(file.path).suffix
        
        if file_extension in RDF_SUFFIXES:
            file_name = pathlib.Path(file.path).stem
            if file_name not in files_group.keys():
                files_group[file_name] = list()
            files_group[file_name].append(file)
        
    #reorder the files by dimension
    for file_name in files_group.keys():
        files_group[file_name].sort(reverse = True, key=lambda x: os.path.getsize(x.path))

    #parse the preferred files, if they are not parsable skip to the other possible file
    #in the list
    for file_name in files_group.keys():
        for file in files_group[file_name]:
            if mineFile(dataset.name, file, dataset_content, f_log, version):
                mined_files.append(file.name) 
                break

    json_serial = json.dumps(dataset_content, indent=4, ensure_ascii=False)

    #writing the json file of the content
    with open(dataset.path+"/dataset_content_rdflibhr.json", "w", encoding="utf-8") as dataset_content_file:
        dataset_content_file.write(json_serial)

    dataset_content_file.close()
    del json_serial
    del dataset_content

    #update the dataset_metadata json file with the mining information
    dataset_metadata["mined_rdflibhr"] = True
    dataset_metadata["mined_files_rdflibhr"] = mined_files

    json_serial = json.dumps(dataset_metadata, indent=4, ensure_ascii=False)

    #writing the json file of the content
    with open(dataset.path+"/dataset_metadata.json", "w", encoding="utf-8") as dataset_metadata_file:
        dataset_metadata_file.write(json_serial)
    
    dataset_metadata_file.close()
    del json_serial
    del dataset_metadata

#3 ore e 40 + 5 ore e 48 min + 4 ore 45 min + 4 ore

if __name__ == "__main__":

    # read the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "datasets_folder", 
        type=str, 
        help="Absolute path to the folder where all the datasets will be downloaded"
    )
    parser.add_argument(
        "version", 
        type=int, 
        help="Version of the parsing (1: labels v1, 2: labels v2)"
    )
    parser.add_argument(
        "--resume", 
        action="store_true",
		help="Add if you want to resume the parsing process"
    )
    args = parser.parse_args()                                                                            

    logging.getLogger("rdflib").setLevel(logging.ERROR)

    global log 
    logging.basicConfig(
        filename="../logs/rdflib_miner_errors.log",
        filemode="a",
        format="%(message)s",
    )
    log = logging.getLogger("rdflib_miner")

    bar = tqdm(total = len(os.listdir(args.datasets_folder)))
    for dataset in os.scandir(args.datasets_folder):
        
        mineDataset(dataset, args.resume, args.version)
        bar.update(1)




    
