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

'''
@param datasets_directory path of directory where are all the datasets
@param error_log_fil_path path of the error log file
@param suffixes array of accettable suffixes
'''
def startCheck(datasets_directory, error_log_fil_path, suffixes):
    
    #open the error log files
    f_log=open(error_log_fil_path, "a")

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

                if (file_suffix not in suffixes):

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

    datasets_directory = "/home/manuel/Tesi/ACORDAR/Test"                   
    #datasets_directory = "/media/manuel/500GBHDD/Tesi/Datasets"                                       #path to the folder of the downloaded datasets
    
    error_log_file_path = os.path.join(scriptDir, '../Log/downloader_error_log.txt')                   #path to the error log file

    suffixes = [".rdf", ".rdfs", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml", ".ntriples"]

    startCheck(datasets_directory, error_log_file_path, suffixes)

if __name__ == "__main__" :
    main()