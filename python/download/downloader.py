'''
This script will download all the datasets indicated in the datasets.json file provided in input
For every dataset it will:
* create the directory for the dataset
* download all the dataset files in the directory
* crete the json file with all the dataset meta-data included the download info in the dataset_metadata.json file 
  and it will remove all the special characters in the name and encode it using UTF-8 encoding.

If there are errors during the download we will log in the dataset_metadata.json which links are not working
and also in a error log file (txt) for a faster retrieve of the errors
'''

import json 
import requests
from tqdm import tqdm
import os
import re
from slugify import slugify
import time

RDF_SUFFIXES = ["rdf", "ttl", "owl", "n3", "nt", "ntriples", "jsonld", "nq", "trig", "trix"]

"""
@param url, string with url 
@param dataset_directory_path path to the dataset directory
@return generated file name
"""
def getFilenNameFromURL(url: str, dataset_directory_path: str) -> str:

    #remove / charachter at the end of the url if present
    if url.endswith("/"):
        url = url[:-1]

    # try to retrieve the resource name and extension from the URL
    resource = url.split("/")[-1]

    resource_split = resource.split(".")
    file_name = resource_split[0]
    file_extension = resource_split[-1]

    # examine the file name and its extension
    extension = None
    if file_extension in RDF_SUFFIXES:
        extension = file_extension
        slug = slugify(file_name)
        file_name = f"{slug}.{extension}"
    else:
        file_name = ".".join(
            list(map(lambda x: slugify(x), resource_split))
        )

    # check that the dataset directory does not already contains a file with the same name
    if os.path.isdir(dataset_directory_path):
        files_in_directory = os.listdir(dataset_directory_path)

        # if a file with the same name has been found generate a timestamp and append file name
        if file_name in files_in_directory:
            time_str = time.strftime("%Y_%m_%d-%I_%M_%S")
            file_name = f"{time_str}-{file_name}"

    return file_name

"""
@param url, string with the url
@return the file name to which the url is referring, if the header is not setted it returns None
"""
def getFileNameFromRequest(url: str) -> str:
    response = requests.head(url)
    response.raise_for_status()

    #extract the file name from the content-disposition header
    if(hasattr(response.headers, "content-disposition")):
        return response.headers["content-disposition"].split("=", -1)[-1]
    
    return None

"""
@param url, url to be downloaded
@param dataset_directory_path, path to the folder in which the downloaded file will be saved
@param file_name: name of the downloaded file
@return True if the download has been completed without errors, otherwise an exception is thrown
"""
def download(url: str, file_name: str, dataset_directory_path: str, ) -> bool:

    response = requests.get(url, stream=True)
    response.raise_for_status()

    download_path = dataset_directory_path + f"/{file_name}"

    # Create a progress bar to track progress while downloading
    total_size_in_bytes = int(response.headers.get("content-length", 0))

    with (
        tqdm(
            total=total_size_in_bytes,
            unit="iB",
            unit_scale=True,
        ) as progress_bar,
        open(download_path, "wb") as target,
    ):
        for data in response.iter_content():
            progress_bar.update(len(data))
            target.write(data)

    return True
  
'''
@param dataset, a dictionary with all the dataset info (metadata and links)
@param datasets_folder_path, path where to store all the datasets
@param f_log log file for the errors
'''
def downloadDataset(dataset: dict, datasets_folder_path: str, f_log: object):

    dataset_id = dataset["dataset_id"]

    #remove duplicate download links 
    dataset_urls = set()
    for url in dataset["download"]:
        dataset_urls.add(url)
    
    #define the dataset directory path
    dataset_directory_path = f"{datasets_folder_path}/dataset-{dataset_id}"

    #create the dataset directory is it does not exists
    if(not os.path.exists(dataset_directory_path)):
        os.makedirs(dataset_directory_path)

    #download dataset files

    downloaded_files = list()
    failed_urls = list()

    for url in dataset_urls:
        try:
            
            #extract the file name from the content-disposition header of a request if available
            #if not we extract the name from the URL
            downloaded_file_name = getFileNameFromRequest(url)

            if downloaded_file_name is None:
                downloaded_file_name = getFilenNameFromURL(url, dataset_directory_path)

            download(url,downloaded_file_name, dataset_directory_path)

            #save the downloaded file into the downloaded entry
            downloaded_files.append({"url": url, "file_name": downloaded_file_name})

        except Exception as err:
            failed_urls.append(url)
            error_message = str(err).replace("\n"," ")
            print("Dataset: "+dataset_id+"\nURL: "+url+"\nError: "+error_message+"\n")
            f_log.write("Dataset: "+dataset_id+"\nURL: "+url+"\nError: "+error_message+"\n\n")

    # serialize metadata and download information, we clean from control charachters title 
    # and description fields 
    metadata = dict()
    metadata["dataset_id"] = dataset_id
    metadata["title"] = re.sub("\\n|\\r|  +", '', dataset.get("title", ""))
    metadata["description"] = re.sub("\\n|\\r|  +", '', dataset.get("description", ""))
    metadata["author"] = dataset.get("author", "")
    metadata["tags"] = dataset.get("tags  ", "")
    metadata["downloaded_urls"] = downloaded_files
    metadata["failed_download_urls"] = failed_urls
    metadata["download_info"] = {
        "downloaded" : len(downloaded_files),
        "total_URLS" : len(dataset_urls)
    }

    # prepare the JSON representation of the metadata
    metadata_json = json.dumps(metadata, ensure_ascii=False, indent=4)

    # path to the file in which the data will be stored
    metadata_file_path = f"{dataset_directory_path}/dataset_metadata.json"

    # write the json data in the file
    metadata_file = open(metadata_file_path, "w", encoding="utf-8")
    metadata_file.write(metadata_json)
    metadata_file.close()
        

def main():
    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    #define the paths 
    datasets_json_path = "/home/manuel/Tesi/ACORDAR/Data/datasets.json"                          #path to the datasets list json file
    #datasets_folder_path = "/home/manuel/Tesi/Datasets"                                          #path to the folder that contains the datasets
    datasets_folder_path = "/home/manuel/Tesi/ACORDAR/Datasets-1"
    error_log_file_path = os.path.join(scriptDir, 'logs/downloader_error_log_1.txt')               #path to the error log file
    resume_row = 0                                                                            #row in the datasets.json file from which resume the download


    error_log_file = None
    #open the error log file
    if resume_row is None:
        error_log_file = open(error_log_file_path, "w", encoding="utf-8")
    else:
        error_log_file = open(error_log_file_path, "a", encoding="utf-8")

    #read the datasets list json file provided
    datasets_list_file = open(datasets_json_path, "r", encoding="utf-8")
    datasets_list = json.load(datasets_list_file, strict=False)
    datasets_list_file.close()

    row_index = 0

    for dataset in datasets_list["datasets"]:
        dataset_id = dataset["dataset_id"]

        if resume_row is None or (resume_row is not None and row_index > resume_row):
            print(f"Processing dataset [ID: {dataset_id}] [INDEX: {row_index}] downloads: {len(dataset['download'])}")
            downloadDataset(dataset, datasets_folder_path, error_log_file)
        else:
            print(f"Already downloaded dataset with ID {dataset_id} - [INDEX: {row_index}]")
        
        row_index += 1
    
    error_log_file.close()


if __name__ == "__main__" : 
    main(); 