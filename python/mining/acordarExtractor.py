"""
Extracts the ACORDAR baseline needed data from all the datasets files with valid suffixes (reported in RDF_SUFFIXES).
It appends extracted data in the `dataset_content.json` file that is contained inside every dataset folder.
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


def getClasses(graph) -> list:
    q = """
    SELECT ?class
    WHERE {
        ?s a ?class .
    }
    """
    match = graph.query(q)

    classes = list()

    for item in match:
        classes.append(item[0])

    return classes


def getEntities(graph) -> list:
    q = """
    SELECT ?s
    WHERE {
        ?s a ?class .
    }
    """
    match = graph.query(q)

    entities = list()

    for item in match:
        entities.append(item[0])

    return entities


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
@param folder name of the dataset folder
@param file that must be mined
@param dataset_content dictionary with the dataset content already mined
@param f_log miner error log file
@return True if the file is mined, else False
'''
def mineFile(folder, file, dataset_content, f_log):
    g = Graph()

    if (os.path.getsize(file.path) / (1024 ** 2)) < FILE_LIMIT_SIZE: 
        try: 
            g.parse(file.path)

            #extract all the info

            classes = getClasses(g)
            entities = getEntities(g)
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
            f_log.write("Dataset: "+folder.name+"\nFile: "+file.name+"\nError: "+str(e)+"\n")
            return False
        except Exception as e :
            f_log.write("Dataset: "+folder.name+"\nFile: "+file.name+"\nError: "+str(e)+"\n")
            return False
    else :
        f_log.write("Dataset: "+folder+"\nFile: "+file+"\nError: Bigger than "+FILE_LIMIT_SIZE+"\n")
        return False
    
    return True

'''
@param folder of the dataset
@param f_log error log file
'''
def mineDataset(folder, f_log):

    dataset_content = {}

    #open the dataset metadata file
    dataset_metadata_file = open(folder.path+"/dataset_metadata.json", "r", encoding="utf-8")
    dataset_metadata = json.load(dataset_metadata_file, strict = False)
    dataset_metadata_file.close()

    if dataset_metadata["mined"]:
        return 

    dataset_content["classes"] = list()
    dataset_content["entities"] = list()
    dataset_content["literals"] = list()
    dataset_content["properties"] = list()

    #print("Mining dataset: "+folder.name)

    mined_files = 0

    for file in os.scandir(folder):
        
        if file.name != "dataset_metadata.json" and file.name != "dataset_content.json":
            if mineFile(folder, file, dataset_content, f_log):
                mined_files+=1 

    json_serial = json.dumps(dataset_content, indent=4, ensure_ascii=False)

    #writing the json file of the content
    with open(folder.path+"/dataset_content.json", "w", encoding="utf-8") as dataset_content_file:
        dataset_content_file.write(json_serial)

    dataset_content_file.close()
    del json_serial
    del dataset_content

    #update the dataset_metadata json file with the mining information
    dataset_metadata["mined"] = True
    dataset_metadata["mined_files"] = mined_files

    json_serial = json.dumps(dataset_metadata, indent=4, ensure_ascii=False)

    #writing the json file of the content
    with open(folder.path+"/dataset_metadata.json", "w", encoding="utf-8") as dataset_metadata_file:
        dataset_metadata_file.write(json_serial)
    
    dataset_metadata_file.close()
    del json_serial
    del dataset_metadata


def main():

    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    datasets_directory_path = "/media/manuel/500GBHDD/Tesi/Datasets"                            #path to the folder of the downloaded datasets
    #datasets_directory_path = "/home/manuel/Tesi/ACORDAR/Datasets"                                       #path to the folder of the downloaded datasets
    error_log_file_path = os.path.join(scriptDir, 'logs/miner_error_log.txt')                    #path to the error log file

    logging.getLogger("rdflib").setLevel(logging.ERROR)

    #open the error log file of the miner
    f_log=open(error_log_file_path, "w+")

    n_dataset = 0
    total_dataset = len(os.listdir(datasets_directory_path))
    skip_datasets = ["dataset-11580"]

    pbar = tqdm(total = total_dataset)

    for folder in os.scandir(datasets_directory_path):

        if folder not in skip_datasets:
        
            mineDataset(folder, f_log)
            n_dataset+=1

            pbar.update(1)

    f_log.close()

if __name__ == "__main__":
    main() 



    
