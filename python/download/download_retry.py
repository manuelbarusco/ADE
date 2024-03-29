'''
This script will retry the download of all the failed download 
reported in the downloader logs.
If an URL now works it will download the file in the correct dataset folder
and it will update the correct dataset_metadata.json file, 
else it will log an error in the same format of the downloader error log

This script will help us in retrying the download from all the URLs that were 
previously unavailable for the downloader. 
'''

import json 
import requests
from tqdm import tqdm
import os
import argparse
from slugify import slugify
import time
import logging

#accepted RDF suffixes
RDF_SUFFIXES = ["rdf", "ttl", "owl", "n3", "nt", "ntriples", "jsonld", "nq", "trig", "trix"]


"""
@param: url, string with url 
@param: dataset_directory_path, path to the dataset directory
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
@param: url, string with the url
@return: the file name to which the url is referring by looking to the headers, if the header is not setted it returns None
"""
def getFileNameFromRequest(url: str) -> str:
    response = requests.head(url)
    response.raise_for_status()

    #extract the file name from the content-disposition header
    if(hasattr(response.headers, "content-disposition")):
        return response.headers["content-disposition"].split("=", -1)[-1]
    
    return None

"""
@param: url, url to be downloaded
@param: file_name, name of the downloaded file
@param: dataset_directory_path, path to the folder in which the downloaded file will be saved
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
@param: datasets_folder_path, path where to store all the datasets
@param: dataset_id, id of the dataset
'''
def retryDownloadDataset(datasets_folder_path:str , dataset_id:int):

    #define the dataset directory path
    dataset_directory_path = f"{datasets_folder_path}/dataset-{str(dataset_id)}"
    
    #define the dataset_metadata.json file path
    metadata_file_path = f"{dataset_directory_path}/dataset_metadata.json"

    #read the dataset_metadata.json file provided
    metadata_file = open(metadata_file_path, "r", encoding="utf-8")
    metadata = json.load(metadata_file, strict=False)
    metadata_file.close()

    #read the error urls in the dataset
    fixed_error_urls = set(metadata["failed_download_urls"])
    error_urls = set(metadata["failed_download_urls"])
    
    downloaded_files = metadata["downloaded_urls"]

    for url in fixed_error_urls:
        try:
            
            #extract the file name from the content-disposition header of a request if available
            #if not we extract the name from the URL
            downloaded_file_name = getFileNameFromRequest(url)

            if downloaded_file_name is None:
                downloaded_file_name = getFilenNameFromURL(url, dataset_directory_path)

            download(url,downloaded_file_name, dataset_directory_path)

            #save the downloaded file into the downloaded entry
            downloaded_files.append({"url": url, "file_name": downloaded_file_name})

            #remove the urls from the error_urls
            error_urls.remove(url)

        except Exception as err:
            error_message = str(err).replace("\n"," ")
            log.warning(
                f"""
                Dataset: {dataset_id}
                URL: {url}
                Error: {error_message}
                """
            )

    # update the dataset_metadata.json file
    metadata["downloaded_urls"] = downloaded_files
    metadata["failed_download_urls"] = list(error_urls)
    metadata["download_info"] = {
        "downloaded" : len(downloaded_files),
        "total_URLS" : len(downloaded_files) + len(error_urls)
    }

    # prepare the JSON representation of the metadata
    metadata_json = json.dumps(metadata, ensure_ascii=False, indent=4)

    # write the json data in the file
    metadata_file = open(metadata_file_path, "w", encoding="utf-8")
    metadata_file.write(metadata_json)
    metadata_file.close()
    

if __name__ == "__main__" : 
    scriptDir = os.path.dirname(os.path.realpath('__file__'))

    # read the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "datasets_folder", 
        type=str, 
        help="Absolute path to the folder where all the datasets will be downloaded"
    )
    parser.add_argument(
        "--start-from",
        type=int,
        help="Start downloading from the given row index in the downloader error log file (included)",
    )
    args = parser.parse_args()         
    
    resume_row = None
    if args.start_from is not None:
        resume_row = args.start_from  

    global log 
    logging.basicConfig(
        filename="logs/download_retry_errors.log",
        filemode="a",
        format="%(message)s",
    )
    log = logging.getLogger("download_retry")

    #path to the downloader error log file
    downloader_log_file_path = os.path.join(scriptDir, 'logs/downloader_errors.log')          

    #read downloader error log file provided
    downloader_log_file = open(downloader_log_file_path, "r", encoding="utf-8")
    
    problem_datasets = set()

    while True:
        line1 = downloader_log_file.readline()

        if not line1:
            break

        downloader_log_file.readline()
        downloader_log_file.readline()
        downloader_log_file.readline()

        dataset_id = int(line1.split(": ")[1].strip("\n"))

        problem_datasets.add(dataset_id)

    downloader_log_file.close()

    print(f"Find: {len(problem_datasets)} datasets with errors")

    problem_datasets = list(problem_datasets)
    problem_datasets = sorted(problem_datasets)
            
    index = 0
    for dataset_id in problem_datasets:
        if resume_row is None or (resume_row is not None and index > resume_row):
            print(f"Processing dataset [ID: {dataset_id}] [INDEX: {index}]")
            retryDownloadDataset(args.datasets_folder, dataset_id)
        else:
            print(f"Already processed dataset with ID {dataset_id} - [INDEX: {index}]")
        index += 1