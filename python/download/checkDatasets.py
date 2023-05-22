'''
Script for checking the datasets downloaded files

We will:
* clean up the file name from the URL noise 
* check the file extension if present in the cleaned up file name

We will log for each dataset:
* which files have not an RDF extension or empty extension
'''

import os
import pathlib
import re
import magic

SUFFIXES = [".rdf", ".rdfs", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml", ".ntriples", ".nq", ".trig", ".trix"]

'''
@param datasets_directory path of directory where are all the datasets
@param error_log_fil_path path of the error log file
'''
def startCheck(datasets_directory, error_log_file_path):
    
    #open the error log files
    f_log=open(error_log_file_path, "a")

    index = 0
    dataset_name = "dataset-0"

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
                if (file_suffix not in SUFFIXES):

                    #recover a possible file type
                    file_type = magic.from_file(path)
                    
                    #Log the file
                    if folder.name != dataset_name:
                        dataset_name = folder.name
                        f_log.write("Dataset: "+dataset_name+"\n")

                    f_log.write("File: "+file.name+"\n")
                    f_log.write("Mime: "+file_type+"\n")
            
        index += 1 

    f_log.close()

def main():

    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    #datasets_directory = "/home/manuel/Tesi/ACORDAR/Datasets"                   
    datasets_directory = "/media/manuel/Tesi/Datasets"                                       #path to the folder of the down    loaded datasets
    
    error_log_file_path = os.path.join(scriptDir, '../logs/checker_error_log.txt')                        #path to the error log file

    startCheck(datasets_directory, error_log_file_path)

if __name__ == "__main__" :
    main()