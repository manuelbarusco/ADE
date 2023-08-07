'''
This script will recover the files that are reported by the check_datasets.py script in its log file
In particular it will check the mime type of every reported file and:
* if the file is an html file it will log it and the extension remain the same
* if the file is a txt file it will check if it is ttl file by checking the @prefix directive in the first lines
* if the file is a xml file it will convert it to rdf

In the log file it will log which files have been recovered and which not
'''

import os
import re
import magic
import argparse
import logging

'''
@param dataset name of the dataset
@param file name of the file to be checked
@param datasets_directory directory where all the dataset are saved
@output rename the file with ttl or rdf
'''
def checkForTTL(dataset, file, datasets_directory):
    file_path = datasets_directory+"/"+dataset+"/"+file
    file = open(file_path,"r")

    line = file.readline()
    return re.search("@prefix", line, re.IGNORECASE)


'''
@param dataset name of the dataset
@param file name of the file to be checked
@param datasets_directory directory where all the dataset are saved
@return true if the file was recovered else false
@output rename the file with ttl or rdf if can be recovered
'''
def recoverFile(dataset, file, datasets_directory): 
    file_path = datasets_directory+"/"+dataset+"/"+file
    mime_type = magic.from_file(file_path, mime=True)

    if "xml" in mime_type:
        os.rename(file_path, str(file_path)+".rdf")
        return True
    elif "html" in mime_type:
        os.rename(file_path, str(file_path)+".html")
        return True
    elif "text" in mime_type:
        if checkForTTL(dataset, file, datasets_directory):
            os.rename(file_path, str(file_path)+".ttl")
            return True
    return False

'''
@param datasets_directory directory where are saved all the datasets
@param checker_error_log_file_path path to the txt errors log file of the dataset checker
@param error_log_file_path path to the error log of the file recover 
'''
def startRecover(datasets_directory,checker_error_log_file_path, error_log_file_path):
    #open the error log files of the dataset checker
    f_log_checker=open(checker_error_log_file_path, "r")

    #open the error log file
    f_log=open(error_log_file_path, "a")

    dataset= ""
    while True:
        line = f_log_checker.readline()

        if(not line):
            break

        #skip blank lines
        if ":" in line:
            #split the line
            fields = line.split(": ")

            if fields[0] == "Dataset":
                dataset = fields[1].strip("\n")
            elif fields[0] == "File":
                file = fields[1].strip("\n")

                split = file.split(".")

                #check if there is no extension available for the file

                if len(split) == 1:

                    if os.path.isfile(datasets_directory+"/"+dataset+"/"+file):
                        if not recoverFile(dataset, file, datasets_directory):
                            mime_type = magic.from_file(datasets_directory+"/"+dataset+"/"+file)

                            log.warning(
                                f"""
                                Dataset: {dataset}\n
                                File: {file}\n
                                Mime: {mime_type}\n
                                """
                            )
                        else:
            
                            log.warning(
                                f"""
                                Recover in Dataset: {dataset}\n
                                File: {file}\n
                                """
                            )
    
    f_log_checker.close()
    f_log.close()

if __name__ == "__main__":
    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    # read the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "datasets_folder", 
        type=str, 
        help="Absolute path to the folder where all the datasets will be downloaded"
    )
    args = parser.parse_args()  
    
    global log 
    logging.basicConfig(
        filename="logs/simple_file_recover.log",
        filemode="a",
        format="%(message)s",
    )
    log = logging.getLogger("simple_file_recover")

    #path to the checker error log file path
    checker_error_log_path = os.path.join(scriptDir, '../download/logs/checker_error_log.txt')       

    startRecover(args.datasets_folder,checker_error_log_path)
