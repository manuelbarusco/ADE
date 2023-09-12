"""
Extracts the ACORDAR baseline needed data from all the datasets files with valid suffixes that are not mined 
by the JENA extractor (with triples deduplication). 
"""

import json
import os
import lightrdf
import argparse
import logging
import re
from tqdm import tqdm

SUFFIXES = ["rdf", "rdfs", "ttl", "owl", "n3", "nt", "jsonld", "xml", "ntriples", "nq", "trig", "trix"]

MAX_TRIPLES = 300000

'''
@param: node, string representation of a RDF graph node
@return True if the node is a string else False
'''
def is_literal(node: str) -> bool:
    return node.startswith('"') and node.endswith('"')

'''
@param: dataset_path, path to the dataset folder
@param: file, path to the RDF file
@param: triples, set of triples
@return True if the file is mined, else False
'''
def tripleDeduplication(dataset_path:str, file: str, triples:set)->bool:

    ext = file.split(".")[-1]

    if ext in SUFFIXES:

        file_path = dataset_path+"/"+file

        #check the file size
        size = os.path.getsize(file_path) / (1024*1024)

        is_big = False
        if size > 500:
            is_big = True

        try: 

            doc = lightrdf.RDFDocument(file_path)

            triple_count = 0 
            #triple deduplication
            for triple in doc.search_triples(None, None, None):
                if is_big and triple_count > MAX_TRIPLES: 
                    break
                triples.add(triple)
                triple_count += 1
        
        except Exception as e :
            error_message = str(e).strip("\n")
            log.warning(f"Dataset: {dataset}\nFile: {file}\nError: {error_message}\n")
            return False

        return True
    
    log.warning(f"Dataset: {dataset}\nFile: {file}\nError: File not RDF\n")
    return False


'''
@param: dataset_directory_path, path to the directory where all the datasets are stored   
@param: dataset, to be considered (string with the dataset name)
@param: errors, list of problem files for jena in the dataset
@param: resume, boolean used for resume mechanism
'''
def mineDataset(datasets_directory_path: str, dataset: str, errors: list, resume: bool):

    dataset_path = datasets_directory_path+"/"+dataset 

    #open the dataset metadata file
    dataset_metadata_file = open(dataset_path+"/dataset_metadata.json", "r", encoding="utf-8")
    dataset_metadata = json.load(dataset_metadata_file, strict = False)
    dataset_metadata_file.close()

    #checking for the resume mechanism
    if "mined_lightrdf_deduplication" in dataset_metadata.keys():
        if dataset_metadata["mined_lightrdf_deduplication"] and resume:
            return

    mined_files = list()

    dataset_content = dict()
    dataset_content["entities"] = list()
    dataset_content["literals"] = list()
    dataset_content["properties"] = list()
    dataset_content["classes"] = list()

    #save all the triples in a set for doing triple deduplication
    triples = set()

    for file in errors:

        if tripleDeduplication(dataset_path, file, triples): 
            mined_files.append(file) 
    

    #read the deduplicated triples
    for triple in triples:
        sub = triple[0]
        prop = triple[1]
        obj = triple[2]

        #clean subject and property from < > 
        sub = re.sub("<|>", "", sub)
        prop = re.sub("<|>", "", prop)
        
        dataset_content["entities"].append(sub)
        
        if "type" in prop.lower() or "a" == prop.lower():
            obj = re.sub("<|>", "", obj)
            dataset_content["properties"].append(prop)
            dataset_content["classes"].append(obj)
            continue

        dataset_content["properties"].append(prop)

        if is_literal(obj):
            dataset_content["literals"].append(obj+"\n")
        else:
            obj = re.sub("<|>", "", obj)
            dataset_content["entities"].append(obj+"\n")


    #update the dataset_metadata json file with the mining information
    dataset_metadata["mined_lightrdf_not_large"] = True
    dataset_metadata["mined_lightrdf_not_large_files"] = mined_files

    #writing the json file of the metaadata
    json_serial = json.dumps(dataset_metadata, indent=4, ensure_ascii=False)
    with open(dataset_path+"/dataset_metadata.json", "w", encoding="utf-8") as dataset_metadata_file:
        dataset_metadata_file.write(json_serial)

    #writing the json file of the content   
    json_serial = json.dumps(dataset_content, indent=4, ensure_ascii=False)
    with open(dataset_path+"/dataset_content_lightrdf_deduplication.json", "w", encoding="utf-8") as dataset_content_file:
        dataset_content_file.write(json_serial)
    
    del json_serial
    del dataset_metadata
    del dataset_content


if __name__ == "__main__":

    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    # read the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "datasets_folder", 
        type=str, 
        help="Absolute path to the folder where all the datasets will be downloaded"
    )
    parser.add_argument(
        "--resume", 
        action="store_true",
		help="Add if you want to resume the parsing process"
    )
    args = parser.parse_args()

    #path to the error log file of Jena Deduplication miner
    error_log_file_path = os.path.join(scriptDir, '../../../java/src/main/java/deduplication/logs/jena_miner_error_log_deduplication.log')                                     

    global log 
    logging.basicConfig(
        filename="../logs/lightrdf_miner_errors.log",
        filemode="a",
        format="%(message)s",
    )
    log = logging.getLogger("lightrdf_miner")

    #open the error log file of the rdflib miner
    f_log=open(error_log_file_path, "r")

    n_dataset = 0

    datasets_files_errors = {}

    while True:
        line1 = f_log.readline()
    
        if not line1:
            break

        line2 = f_log.readline()
        line3 = f_log.readline()

        dataset = line1.split(": ")[1].strip("\n")
        file = line2.split(": ")[1].strip("\n")

        if dataset not in datasets_files_errors.keys():
            datasets_files_errors[dataset] = list() 
    
        datasets_files_errors[dataset].append(file)
        n_dataset += 1
                
    print("Find: "+str(n_dataset)+" datasets with problem files, starts mining ...")

    pbar = tqdm(total = len(datasets_files_errors.keys()))

    for dataset, errors in datasets_files_errors.items():
        mineDataset(args.datasets_folder, dataset, errors, args.resume)
        pbar.update(1)

