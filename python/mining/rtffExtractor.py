"""
Extracts the ACORDAR baseline needed data from all the datasets files with valid rdf suffixes 
that are not parsed by JENA or RDFLib because are broken files: these files may contain invalid 
syntax or other problems
"""

import pathlib
import json
import os
import logging
from tqdm import tqdm
from lxml import etree
import json
import codecs

SUFFIXES = [".rdf", ".rdfs", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml", ".ntriples", ".nq", ".trig", ".trix"]

'''
@param file_path path to the file to be parsed
@param dataset_content dictionary with the elements parsed until now
'''
def parse_as_XML(file_path: str, dataset_content: dict):

    parser = etree.XMLParser(recover=True)

    tree = etree.parse(file_path, parser)

    root = tree.getroot()

    if root == None:
        raise Exception("File cannot be parsed as XML")
    
    for element in root.iter():
        #print("Tag:" + str(element.tag) + " Text:" + str(element.text))
        
        if "Description" in element.tag:
            if len(element.attrib.keys()) > 0:
                if "about" in element.attrib.keys()[0]:
                    dataset_content["entities"].append(element.get(element.attrib.keys()[0]))
        
        elif "type" in element.tag:
            if len(element.attrib.keys()) > 0:
                if "resource" in element.attrib.keys()[0]:
                    dataset_content["classes"].append(element.get(element.attrib.keys()[0]))

        elif "}" in element.tag:
            split = element.tag.split("}")
            property = split[1]
            if property != "RDF" and property != "Description":
                dataset_content["properties"].append(property)
        else:
            dataset_content["properties"].append(element.tag)

        if element.text != None and element.text.strip():
            dataset_content["literals"].append(element.text)


'''
@param element Element object parsable with lxml
@param classes list of classes 
@param entities list of entities
@param properties list of properties
@param literals list of literals
'''
def parseElement(element: object, classes: list, entities: list, properties: list, literals: list):

    for key in element.keys():
        if key=="@id":
            entities.append(element[key])
        elif key=="@type":
            classes.append(element[key])
        
        if isinstance(element[key], str):
            literals.append(element[key])
        elif isinstance(element[key], object):
            parseElement(element[key], classes, entities, properties, literals)

        properties.append(key)

'''
@param file_path path to the file to be parsed
@param dataset_content dictionary with the elements parsed until now
'''
def parse_as_JSON(file_path: str, dataset_content):
    json_file = json.load(codecs.open(file_path, "r", "utf-8-sig"), strict=False)

    graph = json_file["@graph"]

    for element in graph:

        parseElement(element, dataset_content["classes"], dataset_content["entities"], dataset_content["properties"], dataset_content["literals"])


'''
@param dataset_path path to the dataset folder
@param dataset name of the dataset
@param file name of the file that must be mined
@param dataset_content dictionary with the dataset content already mined
@param f_log miner error log file
@return True if the file is mined, else False
'''
def mineFile(dataset_path:str, dataset: str, file: str, dataset_content: dict, f_log: object) -> bool : 

    file_path = dataset_path+"/"+file

    ext = file.split(".")[-1]

    try: 

        if ext == "jsonld":
            parse_as_JSON(file_path, dataset_content)

        if ext == "rdf" or ext == "nt":
            parse_as_XML(file_path, dataset_content)

    except Exception as e :
        f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: "+str(e)+"\n")
        return False
    
    return True

'''
@param dataset_directory_path path to the directory where all the datasets are stored   
@param dataset to be considered (string with the dataset name)
@param errors list of problem files for jena in the dataset
@param f_log error log file
@param resume boolean used for resume mechanism
'''
def mineDataset(datasets_directory_path: str, dataset: str, errors: list, f_log: object, resume: bool):

    dataset_path = datasets_directory_path+"/"+dataset 

    dataset_content = {}

    #open the dataset metadata file
    dataset_metadata_file = open(dataset_path+"/dataset_metadata.json", "r", encoding="utf-8")
    dataset_metadata = json.load(dataset_metadata_file, strict = False)
    dataset_metadata_file.close()

    #checking for the resume mechanism
    if dataset_metadata["mined_rtff"] and resume:
        return 

    dataset_content["classes"] = list()
    dataset_content["entities"] = list()
    dataset_content["literals"] = list()
    dataset_content["properties"] = list()

    #print("Mining dataset: "+folder.name)

    mined_files = 0

    for file in errors:
        
        if mineFile(dataset_path, dataset, file, dataset_content, f_log):
            mined_files+=1 

    json_serial = json.dumps(dataset_content, indent=4, ensure_ascii=False)

    #writing the json file of the content
    with open(dataset_path+"/dataset_content_rtff.json", "w", encoding="utf-8") as dataset_content_file:
        dataset_content_file.write(json_serial)

    dataset_content_file.close()
    del json_serial
    del dataset_content

    #update the dataset_metadata json file with the mining information
    dataset_metadata["mined_rtff"] = True
    dataset_metadata["mined_files_rtff"] = mined_files

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
    
    error_log_file_path = os.path.join(scriptDir, 'logs/rtff_miner_error_log.txt')                #path to the error log file
    rdflib_error_log_file_path = os.path.join(scriptDir, 'logs/rdflib_miner_error_log.txt')       #path to the error log file of the rdflib miner
    resume = False                                                                                #boolean that indicates if the mining must be resumed from the last results

    #open the error log file of the rdflib miner
    f_log_rdflib=open(rdflib_error_log_file_path, "r")

    #open the error log file of the extractor
    if resume: 
        f_log=open(error_log_file_path, "a")
    else:
        f_log=open(error_log_file_path, "w")

    datasets_files_errors = {}

    n_dataset = 0

    while True:
        line1 = f_log_rdflib.readline()
    
        if not line1:
            break

        line2 = f_log_rdflib.readline()
        f_log_rdflib.readline()
    
        dataset = line1.split(": ")[1].strip("\n")
        file = line2.split(": ")[1].strip("\n")

        if dataset not in datasets_files_errors:
            datasets_files_errors[dataset] = list()
            n_dataset += 1 
        
        datasets_files_errors[dataset].append(file)
                
    print("Find: "+str(n_dataset)+" datasets with errors, starts mining ...")

    i = 0 
    for dataset, errors in datasets_files_errors.items():
        mineDataset(datasets_directory_path, dataset, errors, f_log, resume)
        i+=1
        print("Mined: "+str(i)+" datasets over: "+str(n_dataset))

    
    f_log.close()

if __name__ == "__main__":
    main() 



    
