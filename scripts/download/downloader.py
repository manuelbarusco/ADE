'''
This script will download all the datasets indicated in the datasets.json file provided in input
For every dataset it will:
* create the directory for the dataset
* download all the dataset files in the directory
* crete the json file with all the dataset meta-data included the download info (number of urls dowloaded and urls total number)
  in the dataset_metadata.json file and it will remove all the special characters and encode it using UTF-8 encoding

If there are errors during the download we will log in the errors log file: dataset-id, URL and error of the problem
'''

import json 
import requests
from tqdm import tqdm
import os
import re
  
'''
This function will read the json file list and download all the datasets
@param datasets_json_path path to the json file
@param datasets_folder_path path to the folder where to download the datasets
@param error_log_file_path file with all the errors that are thrown during the download
@param resume_row row in the datasets.json file from wich recover the download
'''
def readAndDownload(datasets_json_path, datasets_folder_path,  error_log_file_path, resume_row):

    #open the json file with the datasets list
    f=open(datasets_json_path, "r", encoding="utf-8")

    #open the error log file
    f_log=open(error_log_file_path, "a")

    #load the json object present in the json datasets list
    data = json.load(f,strict=False)

    row = 1

    for dataset in data['datasets']:

        #resume mechanism

        if(row >= resume_row):

            dataset_id = dataset["dataset_id"]
            
            #define the dataset directory path
            dataset_directory_path = datasets_folder_path + "/dataset-" + dataset_id

            #create a directory for all the datasets files if there isn't
            if(not os.path.exists(dataset_directory_path)):
                os.makedirs(dataset_directory_path)

            print("Processing row order dataset: "+ str(row))
            print("\nStart downloading dataset: "+dataset_id)

            dataset_json = createDatasetMetadataJSONObject(dataset) 
            download(dataset["download"], dataset_id, dataset_directory_path, dataset_json, f_log)

            #save the dataset_json object in the dataset_metadata.json file
            #serializing dataset_json object
            json_serial = json.dumps(dataset_json, indent=4, ensure_ascii=False)

            #writing the json file
            with open(dataset_directory_path+"/dataset_metadata.json", "w", encoding="utf-8") as outfile:
                outfile.write(json_serial)

            #free memory by closing the dataset.json file
            del json_serial
            del dataset_json
            outfile.close()

            print("\nEnd downloading dataset: "+dataset_id)

        #free memory
        del dataset

        row += 1

    # Closing files
    f.close()
    f_log.close()

'''
This function will create the dataset_metadata.json associated json object
@param dataset json object of the dataset
'''
def createDatasetMetadataJSONObject(dataset):

    dataset_json = {}

    #add the meta-tags to the dataset.json 
    addMetaTags(dataset, dataset_json)

    #set the mined false for future mining
    dataset_json["mined"]=False

    return dataset_json 

'''
This function add the metatags to the dataset_metadata json object previosly created
@param dataset json object of the dataset
@param dataset_json json object where to write the metatags
'''
def addMetaTags(dataset, dataset_json):
    #check the presence of the metatag before adding

    if "dataset_id" in dataset:
        dataset_json["dataset_id"] = dataset["dataset_id"]

    if "title" in dataset:
        title = re.sub("\\n|\\r|  +", '', str(dataset["title"]))
        dataset_json["title"] = title

    if "description" in dataset:
        description = re.sub("\\n|\\r|  +", '', str(dataset["description"]))
        dataset_json["description"] = description

    if "author" in dataset:
        dataset_json["author"] = dataset["author"]

    if "tags" in dataset:
        dataset_json["tags"] = dataset["tags"]

'''
This function will download one dataset file
@param url array with all the urls to the datasets
@param dataset_id id of the dataset
@param dataset_directory_path path where the dataset is downloaded 
@param dataset_json dict object to be serialized into a json object and file
@param f_log log file for all the errors
'''
def download(urls, dataset_id, dataset_directory_path, dataset_json, f_log):

    downloaded_files = 0 

    for url in urls: 

        try:
            #do the request
            r = requests.get(url, stream=True)

            #define the filename 
            filename=""

            #extract the file name from the content-disposition header
            if(hasattr(r.headers, "content-disposition")):
                filename = r.headers["content-disposition"].split("=", -1)[-1]
            else:
                #extract file name from the URL
                if url.find('/'):
                    filename = url.rsplit('/', 1)[1]

                #escape not valid caracthers in the url
                match = re.search("[:#?&=%*]", filename)

                if match:
                    filename = filename[0:match.start()]

                #check for empty string and assign a default name
                if filename == "":
                    filename = "file_url_"+str(downloaded_files); 

            #download the dataset
            chunkSize = 1024

            final_path = dataset_directory_path+"/"+filename

            print("\nDownloading from url: "+url)

            f = open(final_path, 'wb') 
            pbar = tqdm( unit="B", total=int( r.headers['Content-Length'] ) )
            for chunk in r.iter_content(chunk_size=chunkSize): 
                if chunk: 
                    pbar.update (len(chunk))
                    f.write(chunk)
            
            #close the  downloaded file
            f.close()
            downloaded_files += 1

        except (requests.ConnectionError,requests.HTTPError,requests.exceptions.RequestException) as e:
            error_message = str(e).replace("\n"," ")
            print("Dataset: "+dataset_id+"\nURL: "+url+"\nError: "+error_message+"\n")
            f_log.write("Dataset: "+dataset_id+"\nURL: "+url+"\nError: "+error_message+"\n")
        
        except (KeyError) as e:
            #if the content-length header is not present

            for chunk in r.iter_content(chunk_size=chunkSize): 
                if chunk: 
                    f.write(chunk)

            #close the  downloaded file
            f.close()
            downloaded_files += 1

    dataset_json["download_info"] = {"downloaded": downloaded_files, "total_URLS": len(urls)}
        

def main():
    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    datasets_json_path = "/home/manuel/Tesi/ACORDAR/Data/datasets.json"                          #path to the datasets list json file
    #datasets_folder_path = "/media/manuel/500GBHDD/Tesi/Datasets"                                #path to the folder that contains the datasets

    datasets_folder_path = "/home/manuel/Tesi/ACORDAR/Datasets"                                #path to the folder that contains the datasets

    error_log_file_path = os.path.join(scriptDir, 'logs/downloader_error_log.txt')               #path to the error log file

    resume_row = 1

    readAndDownload(datasets_json_path, datasets_folder_path, error_log_file_path, resume_row)

if __name__ == "__main__" : 
    main(); 