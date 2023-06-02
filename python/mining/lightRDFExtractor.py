"""
Extracts the ACORDAR baseline needed data from all the datasets files with valid suffixes that are not mined from by JENA 
or RDFLib because the file was too big
"""

import json
import os
import lightrdf

SUFFIXES = [".rdf", ".rdfs", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml", ".ntriples", ".nq", ".trig", ".trix"]


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
def mineFile(dataset_path:str, dataset: str, file: str, dataset_content: dict, f_log: object) -> bool : 
    classes = dataset_content["classes"]
    entities = dataset_content["entities"]
    literals = dataset_content["literals"]
    properties = dataset_content["properties"]

    ext = file.split(".")[-1]

    if ext in SUFFIXES:

        file_path = dataset_path+"/"+file

        try: 

            doc = lightrdf.RDFDocument(file_path)

            for triple in doc.search_triples(None, None, None):
                sub = triple[0]
                prop = triple[1]
                obj = triple[2]

                entities.append(sub)

                if "type" in prop.lower() or "a" == prop.lower():
                    classes.append(obj)
                    continue

                properties.append(prop)

                if is_literal(obj):
                    literals.append(obj)
                else:
                    entities.append(obj)

        except Exception as e :
            f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: "+str(e)+"\n")
            return False
        
        return True
    
    f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: File not RDF\n")
    return False


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
    with open(dataset_path+"/dataset_content_lightrdf.json", "w", encoding="utf-8") as dataset_content_file:
        dataset_content_file.write(json_serial)

    dataset_content_file.close()
    del json_serial
    del dataset_content

    #update the dataset_metadata json file with the mining information
    dataset_metadata["mined_lightrdf"] = True
    dataset_metadata["mined_files_lightrdf"] = mined_files

    json_serial = json.dumps(dataset_metadata, indent=4, ensure_ascii=False)

    #writing the json file of the content
    with open(dataset_path+"/dataset_metadata.json", "w", encoding="utf-8") as dataset_metadata_file:
        dataset_metadata_file.write(json_serial)
    
    dataset_metadata_file.close()
    del json_serial
    del dataset_metadata


def main():

    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    #datasets_directory_path = "/media/manuel/Tesi/Datasets"                                      #path to the folder of the downloaded datasets
    datasets_directory_path = "/home/manuel/Tesi/ACORDAR/Datasets"                                #path to the folder of the downloaded datasets
    
    error_log_file_path = os.path.join(scriptDir, 'logs/lightrdf_miner_error_log.txt')            #path to the error log file
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
        line3 = f_log_rdflib.readline()
    
        if "Bigger than" in line3:
            dataset = line1.split(": ")[1].strip("\n")
            file = line2.split(": ")[1].strip("\n")

            if dataset not in datasets_files_errors:
                datasets_files_errors[dataset] = list()
                n_dataset += 1 
            
            datasets_files_errors[dataset].append(file)
                
    print("Find: "+str(n_dataset)+" datasets with big files, starts mining ...")

    i = 0 
    for dataset, errors in datasets_files_errors.items():
        mineDataset(datasets_directory_path, dataset, errors, f_log, resume)
        i+=1
        print("Mined: "+str(i)+" datasets over: "+str(n_dataset))

    
    f_log.close()

if __name__ == "__main__":
    main() 
