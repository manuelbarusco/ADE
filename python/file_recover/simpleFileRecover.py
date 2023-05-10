'''
This script will recover the files that are reported by the dataset_checker in its log file
In particular it will check the mime type of every reported file and:
* if the file is an html file it will log it and the extension remain the same
* if the file is a txt file it will check if it is ttl file by checking the @prefix directive in the fist lines
* if the file is a xml file it will convert it to rdf

In the log file it will log which file have been recovered and which not
'''

import os
import re
import magic

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

        #split the line
        fields = line.split(": ")

        if fields[0] == "Dataset":
            dataset = fields[1].strip("\n")
        elif fields[0] == "File":
            file = fields[1].strip("\n")
            if os.path.isfile(datasets_directory+"/"+dataset+"/"+file):
                if not recoverFile(dataset, file, datasets_directory):
            
                    f_log.write("Dataset: "+dataset+"\n")
                    f_log.write("File: "+file+"\n")
                    
                    mime_type = magic.from_file(datasets_directory+"/"+dataset+"/"+file)
                    f_log.write("Mime: "+mime_type+"\n")
                else:

                    f_log.write("Recover in Dataset: "+dataset+"\n")
                    f_log.write("File: "+file+"\n")
    
    f_log_checker.close()
    f_log.close()



def main():
    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    #datasets_directory = "/home/manuel/Tesi/ACORDAR/Datasets" 
    datasets_directory = "/media/manuel/500GBHDD/Tesi/Datasets"                                   #path to the folder of the downloaded datasets
    checker_error_log_file_path = os.path.join(scriptDir, 'logs/checker_error_log.txt')           #path to the error log file
    error_log_file_path = os.path.join(scriptDir, 'logs/recover_error_log.txt')                        #path to the error log file

    startRecover(datasets_directory,checker_error_log_file_path, error_log_file_path)

if __name__ == "__main__":
    main()
