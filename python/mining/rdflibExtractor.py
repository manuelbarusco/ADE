"""
Extracts the ACORDAR baseline needed data from all the datasets files with valid suffixes that are not mined from JENA

"""

import pathlib
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF
import json
import os
import logging
from tqdm import tqdm


SUFFIXES = [".rdf", ".rdfs", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml", ".ntriples", ".nq", ".trig", ".trix"]

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
def mineFile(dataset_path:str, dataset:str, file:str, dataset_content:dict, f_log:object) -> bool: 
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
            f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: "+str(e)+"\n")
            return False
        except Exception as e :
            f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: "+str(e)+"\n")
            return False
    else :
        f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: Bigger than "+str(FILE_LIMIT_SIZE)+"\n")
        return False
    
    return True

'''
@param dataset_directory_path path to the directory where all the datasets are stored   
@param dataset to be considered (string with the dataset name)
@param errors list of problem files for jena in the dataset
@param f_log error log file
@param resume boolean used for resume mechanism
'''
def mineDataset(datasets_directory_path:str, dataset:str, errors:list, f_log:object, resume:bool):

    dataset_path = datasets_directory_path+"/"+dataset 

    #open the dataset metadata file
    dataset_metadata_file = open(dataset_path+"/dataset_metadata.json", "r", encoding="utf-8")
    dataset_metadata = json.load(dataset_metadata_file, strict = False)
    dataset_metadata_file.close()

    #checking for the resume mechanism
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
        
        if mineFile(dataset_path, dataset, file, dataset_content, f_log):
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


def main():

    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    datasets_directory_path = "/media/manuel/Tesi/Datasets"                                       #path to the folder of the downloaded datasets
    #datasets_directory_path = "/home/manuel/Tesi/ACORDAR/Datasets"                               #path to the folder of the downloaded datasets
    error_log_file_path = os.path.join(scriptDir, 'logs/rdflib_miner_error_log.txt')              #path to the error log file
    jena_error_log_file_path = os.path.join(scriptDir, 'logs/jena_miner_error_log.txt')           #path to the error log file of the jena miner
    resume = False                                                                                #boolean that indicates if the mining must be resumed from the last results

    logging.getLogger("rdflib").setLevel(logging.ERROR)

    #open the error log file of the jena miner
    f_log_jena=open(jena_error_log_file_path, "r")

    #open the error log file of the extractor
    if resume: 
        f_log=open(error_log_file_path, "a")
    else:
        f_log=open(error_log_file_path, "w")

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
        mineDataset(datasets_directory_path, dataset, errors, f_log, resume)
        i+=1
        print("Mined: "+str(i)+" datasets over: "+str(n_dataset))

    
    f_log.close()

if __name__ == "__main__":
    main() 



    
