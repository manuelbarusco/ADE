"""
Extracts the ACORDAR baseline needed data from all the datasets files with valid suffixes that are not mined by JENA 
"""

import json
import os
import lightrdf
import argparse
import logging

SUFFIXES = ["rdf", "rdfs", "ttl", "owl", "n3", "nt", "jsonld", "xml", "ntriples", "nq", "trig", "trix"]

MAX_ROWS = 300000

def is_literal(node: str) -> bool:
    return node.startswith('"') and node.endswith('"')


'''
@param dataset_path path to the dataset folder
@param dataset name of the dataset
@param file name of the file that must be mined
@param dataset_content dictionary with the dataset content already mined
@param f_log miner error log file
@return True if the file is mined, else False
'''
def mineFile(dataset_path:str, dataset: str, file: str,dataset_content: dict, f_log: object) -> bool : 

    ext = file.split(".")[-1]

    if ext in SUFFIXES:

        file_path = dataset_path+"/"+file

        try: 

            doc = lightrdf.RDFDocument(file_path)
            
            for triple in doc.search_triples(None, None, None):
                sub = triple[0]
                prop = triple[1]
                obj = triple[2]

                dataset_content["entities"].append(sub)

                if "type" in prop.lower() or "a" == prop.lower():
                    dataset_content["properties"].append(prop)
                    dataset_content["classes"].append(obj)
                    continue

                dataset_content["properties"].append(prop)

                if is_literal(obj):
                    dataset_content["literals"].append(obj+"\n")
                else:
                    dataset_content["entities"].append(obj+"\n")

        except Exception as e :
            error_message = str(e).strip("\n")
            f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: "+error_message+"\n")
            return False

        return True
    
    f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: File not RDF\n")
    return False

"""
@param dataset_path path to the dataset folder
@param file name of the file that must be mined
@return True if the file is too big (bigger than 4 GB), else false
"""
def checkFileDimension(dataset_path, file):
    file_path = dataset_path+"/"+file
    return (os.path.getsize(file_path) / (1024 ** 3)) > 4

'''
@param dataset_directory_path path to the directory where all the datasets are stored   
@param dataset to be considered (string with the dataset name)
@param errors list of problem files for jena in the dataset
@param f_log error log file
@param resume boolean used for resume mechanism
'''
def mineDataset(datasets_directory_path: str, dataset: str, errors: list, f_log: object, resume: bool):

    dataset_path = datasets_directory_path+"/"+dataset 

    #open the dataset metadata file
    dataset_metadata_file = open(dataset_path+"/dataset_metadata.json", "r", encoding="utf-8")
    dataset_metadata = json.load(dataset_metadata_file, strict = False)
    dataset_metadata_file.close()

    #checking for the resume mechanism
    if "mined_lightrdf" in dataset_metadata.keys():
        if dataset_metadata["mined_lightrdf"] and resume:
            return

    print("Mining dataset: "+dataset)

    mined_files = list()

    dataset_content = dict()
    dataset_content["entities"] = list()
    dataset_content["literals"] = list()
    dataset_content["properties"] = list()
    dataset_content["literals"] = list()

    for file in errors:
        
        if mineFile(dataset_path, dataset, file, dataset_content, f_log):
            mined_files.append(file) 

    #update the dataset_metadata json file with the mining information
    dataset_metadata["mined_lightrdf"] = True
    dataset_metadata["mined_files_lightrdf"] = mined_files

    json_serial = json.dumps(dataset_metadata, indent=4, ensure_ascii=False)

    #writing the json file of the metaadata
    with open(dataset_path+"/dataset_metadata.json", "w", encoding="utf-8") as dataset_metadata_file:
        dataset_metadata_file.write(json_serial)

    json_serial = json.dumps(dataset_content, indent=4, ensure_ascii=False)
    #writing the json file of the content
    with open(dataset_path+"/dataset_content_lightrdf.json", "w", encoding="utf-8") as dataset_content_file:
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

    #path to the error log file of the rdflib miner
    error_log_file_path = os.path.join(scriptDir, 'logs/jena_miner_error_log.txt')       #path to the error log file of the rdflib miner                                                               

    global log 
    logging.basicConfig(
        filename="logs/lightrdf_miner_errors.log",
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
    
        if "Bigger" not in line3:

            dataset = line1.split(": ")[1].strip("\n")
            file = line2.split(": ")[1].strip("\n")

            if dataset in datasets_files_errors:
                datasets_files_errors[dataset].append(file)
            else:
                datasets_files_errors[dataset] = list() 
                datasets_files_errors[dataset].append(file)
                n_dataset += 1
                
    print("Find: "+str(n_dataset)+" datasets with big files, starts mining ...")

    print(datasets_files_errors)

    i = 0 
    for dataset, errors in datasets_files_errors.items():
        mineDataset(args.datasets_folder, dataset, errors, log, args.resume)
        i+=1
        print("Mined: "+str(i)+" datasets over: "+str(n_dataset))

