'''
This script check if a file contains a UTF-8 BOM sequence at the beginning.
This convention sometimes give problems to the RDFLib parsing phase, so we want to remove it.
'''

import os
import pathlib
import argparse

RDF_SUFFIXES = [".rdf", ".rdfs", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml", ".ntriples", ".nq", ".trig", ".trix"]

"""
@param: path_to_file, path to the RDF file
@return true if the file contains the BOM prefix
"""
def contains_utf8_bom(path_to_file: str) -> bool:
    file = open(path_to_file, "rb")
    #read the first 3 bytes in the file and check if they are the UTF-8 BOM sequence
    bom_bytes = file.read(3)
    file.close()
    return bom_bytes == b"\xEF\xBB\xBF"

"""
@param: path_to_file, path to the RDF file
@return true if the BOM prefix was correctly removed
"""
def remove_utf8_bom(path_to_file: str) -> bool:
    file = open(path_to_file, "r", encoding="utf-8-sig")
    file_content = file.read()
    file.close()

    file = open(path_to_file, "w", encoding="utf-8")
    file.write(file_content)
    file.close()

"""
@param: datasets_directory_path, path to the folder where all the  datasets are stored
"""
def bomChecking(datasets_directory_path: str):
    for dataset_folder in os.scandir(datasets_directory_path):
        for file in os.scandir(dataset_folder):
            file_suffix = pathlib.Path(file.path).suffix

            if file_suffix in RDF_SUFFIXES:
                if contains_utf8_bom(file.path):
                    print("REMOVING UTF-U BOM FROM: "+file.path)
                    remove_utf8_bom(file.path)


if __name__ == "__main__":

    # read the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "datasets_folder", 
        type=str, 
        help="Absolute path to the folder where all the datasets are stored"
    )
    args = parser.parse_args()  

    bomChecking(args.datasets_folder)
