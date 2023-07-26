"""
This script extracts the ACORDAR baseline needed data from all the datasets files 
with valid RDF suffixes that are not mined from JENA. The file to be parsed must have a 
limit size of 100MB due to the big RAM usage of RDFLib and for the RAM limitations of my computing system.
"""

import pathlib
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF
import json
import os
import logging
import argparse

RDF_SUFFIXES = [".rdf", ".rdfs", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml", ".ntriples", ".nq", ".trig", ".trix"]

FILE_LIMIT_SIZE = 100


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


def getClassesAndEntities(graph) -> dict:
    q = """
    SELECT ?class ?s
    WHERE {
        ?s a ?class .
    }
    """
    match = graph.query(q)

    classes = list()
    entities = list()

    for item in match:
        classes.append(item[0])
        entities.append(item[1])

    return classes, entities
    

def getProperties(graph) -> list:
    q = """
    SELECT ?p
    WHERE {
        ?s ?p ?o .
    }
    """
    match = graph.query(q)

    properties = list()

    for item in match:
        properties.append(item[0])

    return properties

'''
@param dataset_path path to the dataset folder
@param dataset name of the dataset
@param file name of the file that must be mined
@param dataset_content dictionary with the dataset content already mined
@param f_log miner error log file
@return True if the file is mined, else False
'''
def mineFile(dataset_path:str, dataset:str, file:str, dataset_content:dict) -> bool: 
    g = Graph()

    file_path = dataset_path+"/"+file

    if (os.path.getsize(file_path) / (1024 ** 2)) < FILE_LIMIT_SIZE: 
        try: 
            g.parse(file_path)

            #extract all the info

            classes, entities = getClassesAndEntities(g)
            literals = getLiterals(g)
            properties = getProperties(g)

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
            log.warning(f"Dataset: {dataset}\nFile: {file}\nError: {error}\n")
            return False
        except Exception as e :
            error = str(e).strip("\n")
            log.warning(f"Dataset: {dataset}\nFile: {file}\nError: {error}\n")
            return False
    else :
        log.warning(f"Dataset: {dataset}\nFile: {file}\nError: Bigger than {str(FILE_LIMIT_SIZE)}\n")
        return False
    
    return True

'''
@param dataset_directory_path path to the directory where all the datasets are stored   
@param dataset to be considered (string with the dataset name)
@param errors list of problem files for jena in the dataset
@param f_log error log file
@param resume boolean used for resume mechanism
'''
def mineDataset(datasets_directory_path:str, dataset:str, errors:list, resume:bool):

    print("Mining")

    dataset_path = datasets_directory_path+"/"+dataset 

    #open the dataset metadata file
    dataset_metadata_file = open(dataset_path+"/dataset_metadata.json", "r", encoding="utf-8")
    dataset_metadata = json.load(dataset_metadata_file, strict = False)
    dataset_metadata_file.close()

    #checking for the resume mechanism
    if "mined_rdflib" in dataset_metadata.keys():
        if dataset_metadata["mined_rdflib"] and resume:
            return 

    dataset_content = dict()

    dataset_content["classes"] = list()
    dataset_content["entities"] = list()
    dataset_content["literals"] = list()
    dataset_content["properties"] = list()

    #print("Mining dataset: "+folder.name)

    mined_files = list()

    for file in errors:
        
        if mineFile(dataset_path, dataset, file, dataset_content):
            mined_files.append(file) 

    json_serial = json.dumps(dataset_content, indent=4, ensure_ascii=False)

    #writing the json file of the content
    with open(dataset_path+"/dataset_content_rdflib.json", "w", encoding="utf-8") as dataset_content_file:
        dataset_content_file.write(json_serial)

    dataset_content_file.close()
    del json_serial
    del dataset_content

    #update the dataset_metadata json file with the mining information
    dataset_metadata["mined_rdflib"] = True
    dataset_metadata["mined_files_rdflib"] = mined_files

    json_serial = json.dumps(dataset_metadata, indent=4, ensure_ascii=False)

    #writing the json file of the content
    with open(dataset_path+"/dataset_metadata.json", "w", encoding="utf-8") as dataset_metadata_file:
        dataset_metadata_file.write(json_serial)
    
    dataset_metadata_file.close()
    del json_serial
    del dataset_metadata


if __name__ == "__main__":
    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    # read the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "datasets_folder", type=str, help="Absolute path to the folder where all the datasets will be downloaded"
    )
    parser.add_argument(
        "--resume", 
        action="store_true",
		help="Add if you want to resume the parsing process"
    )
    args = parser.parse_args()

    #path to the error log file of the jena miner
    jena_error_log_file_path = os.path.join(scriptDir, '../../java/src/main/java/logs/jena_miner_error_log.txt')          
                                                                                

    logging.getLogger("rdflib").setLevel(logging.ERROR)

    global log 
    logging.basicConfig(
        filename="logs/rdflib_miner_errors.log",
        filemode="a",
        format="%(message)s",
    )
    log = logging.getLogger("rdflib_miner")

    #open the error log file of the jena miner
    f_log_jena=open(jena_error_log_file_path, "r")

    datasets_files_errors = {}

    n_dataset = 0

    while True:
        line1 = f_log_jena.readline()
    
        if not line1:
            break

        line2 = f_log_jena.readline()
        f_log_jena.readline()
    
        dataset = line1.split(": ")[1].strip("\n")
        file = line2.split(": ")[1].strip("\n")

        if dataset in datasets_files_errors:
            datasets_files_errors[dataset].append(file)
        else:
            datasets_files_errors[dataset] = list() 
            datasets_files_errors[dataset].append(file)
            n_dataset += 1
                
    print("Find: "+str(n_dataset)+" datasets with errors, starts mining ...")

    i = 0 
    for dataset, errors in datasets_files_errors.items():
        mineDataset(args.datasets_folder, dataset, errors, args.resume)
        i+=1
        print("Mined: "+str(i)+" datasets over: "+str(n_dataset))





    
