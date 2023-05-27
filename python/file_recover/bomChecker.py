'''
This script check if a file contains a UTF-8 BOM sequence at the beginning.
This convention sometimes give problem to the RDFLib parsing phase
'''

import os
import pathlib

SUFFIXES = [".rdf", ".rdfs", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml", ".ntriples", ".nq", ".trig", ".trix"]

def contains_utf8_bom(path_to_file: str) -> bool:
    file = open(path_to_file, "rb")
    #read the first 3 bytes in the file and check if they are the UTF-8 BOM sequence
    bom_bytes = file.read(3)
    file.close()
    return bom_bytes == b"\xEF\xBB\xBF"

def remove_utf8_bom(path_to_file: str) -> bool:
    file = open(path_to_file, "r", encoding="utf-8-sig")
    file_content = file.read()
    file.close()

    file = open(path_to_file, "w", encoding="utf-8")
    file.write(file_content)
    file.close()

def bomChecking(datasets_directory_path: str):
    for dataset_folder in os.scandir(datasets_directory_path):
        for file in os.scandir(dataset_folder):
            file_suffix = pathlib.Path(file.path).suffix

            if file_suffix in SUFFIXES:
                #print(file.path)
                if contains_utf8_bom(file.path):
                    print("REMOVING UTF-U BOM FROM: "+file.path)
                    remove_utf8_bom(file.path)


def main():

    datasets_directory_path = "/media/manuel/Tesi/Datasets"             #path to the folder of the downloaded datasets
    #datasets_directory_path = "/home/manuel/Tesi/ACORDAR/Datasets"             #path to the folder of the downloaded datasets
    bomChecking(datasets_directory_path)

if __name__ == "__main__":
    main()
