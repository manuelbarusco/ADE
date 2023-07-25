'''
Script for checking the datasets downloaded files

It will:
* clean up the file name from the URL noise 
* check the file extension if present in the cleaned up file name

It will log for each dataset:
* which files have not an RDF extension or empty extension
'''

import os
import pathlib
import re
import magic
import argparse
import logging

#accepted RDF suffixes
RDF_SUFFIXES = [".rdf", ".ttl", ".owl", ".n3", ".nt", ".ntriples", ".jsonld", ".nq", ".trig", ".trix"]

'''
@param: datasets_directory, path of directory where are all the dataset
'''
def startCheck(datasets_directory:str):

    index = 0
    dataset_name = "dataset-0"

    first = True

    for folder in os.scandir(datasets_directory):

        if index % 1000 == 0:
            print("Checked: "+str(index)+" datasets")

        dataset_path = datasets_directory+"/"+folder.name
            
        for file in os.scandir(folder):

            #not consider the dataset.json file

            if file.name != "dataset_metadata.json":

                path = pathlib.Path(file)

                #clean up the file name and highlight the file extension: check for REST API syntax
                
                filename = file.name

                match = re.search("\.+[?\/_#:]", filename)

                if(match):
                    filename = filename[0:match.start()]

                path = dataset_path+"/"+filename

                #check for the file extension (file suffix)

                file_suffix = pathlib.Path(path).suffix

                #log files that have not a valid RDF extension or empty extension
                if (file_suffix not in RDF_SUFFIXES):

                    #recover a possible file type
                    file_type = magic.from_file(path)
                    
                    #Log the file
                    if folder.name != dataset_name:
                        dataset_name = folder.name
                        if first:
                            log.warning(f"Dataset: {dataset_name}")
                            first = False
                        else:
                            log.warning(f"\nDataset: {dataset_name}")
                    log.warning(f"File: {file.name} \nMime: {file_type}")
            
        index += 1 


if __name__ == "__main__" :
    # read the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "datasets_folder", type=str, help="Absolute path to the folder where all the datasets will be downloaded"
    )
    args = parser.parse_args()     

    global log 
    logging.basicConfig(
        filename="logs/download_checker.log",
        filemode="a",
        format="%(message)s"
    )
    log = logging.getLogger("checker")

    startCheck(args.datasets_folder)