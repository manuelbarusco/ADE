"""
Extracts the ACORDAR baseline needed data from all the datasets files with valid suffixes
The purpose of this extractor is to extract a human readable version of the triples 
by using the rdfs:label and rdfs:comment properties
"""

import pathlib
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF
import json
import os
import logging
import re 
from tqdm import tqdm


SUFFIXES = [".rdf", ".rdfs", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml", ".ntriples", ".nq", ".trig", ".trix"]

FILE_LIMIT_SIZE = 100

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

def getClassesAndEntities(graph) -> dict:
    q = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?s ?ls ?cs ?c ?lc ?cc
    WHERE {
        ?s a ?c
        
        #retrieve a possible label or comment
        #for the class or the entity
        OPTIONAL{
            ?c rdfs:label ?lc 
        }
        OPTIONAL{
            ?c rdfs:comment ?cc
        }
        
        OPTIONAL{
            ?s rdfs:label ?ls . 
        }
        OPTIONAL{
            ?s rdfs:comment ?cs
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

def getProperties(graph) -> list:
    q = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?p ?l ?c  
    WHERE { 
        ?s ?p ?o .
        OPTIONAL {
            ?p rdfs:label ?l
        }
        OPTIONAL {
            ?p rdfs:label ?c
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
@param dataset name of the dataset
@param file object of type DirEntry
@param dataset_content dictionary with the dataset content already mined
@param f_log miner error log file
@return True if the file is mined, else False
'''
def mineFile(dataset:str, file:object, dataset_content:dict, f_log:object) -> bool: 
    g = Graph()

    if (os.path.getsize(file.path) / (1024 ** 2)) < FILE_LIMIT_SIZE: 
        try: 
            g.parse(file.path)

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
@param dataset object of type DirEntry   
@param f_log error log file
@param resume boolean used for resume mechanism
'''
def mineDataset(dataset:object, f_log:object, resume:bool):


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
    #version but with different extensions of the same file

    files_group = dict()
    for file in os.scandir(dataset.path):
        file_extension = pathlib.Path(file.path).suffix
        
        if file_extension in SUFFIXES:
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
            if mineFile(dataset.name, file, dataset_content, f_log):
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


def main():

    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    datasets_directory_path = "/media/manuel/Tesi/Datasets"                                     #path to the folder of the downloaded datasets
    #datasets_directory_path = "/home/manuel/Tesi/ACORDAR/Datasets"                               #path to the folder of the downloaded datasets
    error_log_file_path = os.path.join(scriptDir, 'logs/rdflibhr_miner_error_log.txt')           #path to the error log file
    resume = True                                                                               #boolean that indicates if the mining must be resumed from the last results

    logging.getLogger("rdflib").setLevel(logging.ERROR)

    #open the error log file of the extractor
    if resume: 
        f_log=open(error_log_file_path, "a")
    else:
        f_log=open(error_log_file_path, "w")

    bar = tqdm(total = len(os.listdir(datasets_directory_path)))
    for dataset in os.scandir(datasets_directory_path):
        
        mineDataset(dataset, f_log, resume)
        bar.update(1)
    
    f_log.close()

if __name__ == "__main__":
    main() 



    
