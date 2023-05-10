'''
This script will retry the download of all the previsous failed download 
reported in the downloader logs.
If an URL now works it will download the file in the correct dataset folder
and it will log it, else it will log an error in the same format of the downloader error logs
'''

import requests
from tqdm import tqdm
import os
import re
import json

'''
@param datasets_folder_path path to the folder that contains the datasets
@param error_log_file_path path to the downloader errors log file
@param download_retry_log_path path to the download retry logs
'''
def retryDownloads(datasets_folder_path, error_log_file_path, download_retry_log_path):
    #open the needed files
    error_log_file = open(error_log_file_path, "r")
    download_retry_log = open(download_retry_log_path, "a")

    while True:
        line1 = error_log_file.readline()

        if(not line1):
            break

        line2 = error_log_file.readline()
        line3 = error_log_file.readline()

        dataset_id = line1.split(": ")[1].strip("\n")
        url = line2.split(": ")[1].strip("\n")

        dataset_directory_path = datasets_folder_path + "/dataset-" + dataset_id

        #retry the download
        download(url, dataset_id, dataset_directory_path, download_retry_log)

'''
@param url where to retry the download
@param dataset_id id of the dataset
@param dataset_directory_path where to download the file
@param download_retry_log file where to log the retry logs
'''
def download(url, dataset_id, dataset_directory_path, download_retry_log):
    #create a simple index for the default name of the file just in case of errors for the name
    url_i = len(os.listdir(dataset_directory_path))-1

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

            #check for empty string
            if filename == "":
                filename = "file_url_"+str(url_i); 

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
        success = True
        download_retry_log.write("Recovered download in dataset: "+dataset_id+"\nURL: "+url+"\n")

    except (requests.ConnectionError,requests.HTTPError,requests.exceptions.RequestException) as e:
        print("Dataset: "+dataset_id+"\nURL: "+url+"\nError: "+str(e)+"\n")
        download_retry_log.write("Dataset: "+dataset_id+"\nURL: "+url+"\nError: "+str(e)+"\n")
        success = False
    
    except (KeyError) as e:
        #if the content-length header is not present

        for chunk in r.iter_content(chunk_size=chunkSize): 
            if chunk: 
                f.write(chunk)
        #close the  downloaded file
        f.close()
        success = True

    #check if the download was successfull
    if success:

        #update the dataset.json file
        dataset_json_file=open(dataset_directory_path+"/dataset_metadata.json", "r", encoding="utf-8")
        dataset_json = json.load(dataset_json_file,strict=False)
        dataset_json_file.close()

        dataset_json_file=open(dataset_directory_path+"/dataset_metadata.json", "w", encoding="utf-8")

        dataset_json["download_info"]["downloaded"] += 1

        #serializing dataset_json object
        dataset_json = json.dumps(dataset_json, indent=4)

        #writing the json file
        dataset_json_file.write(dataset_json)

        dataset_json_file.close()
        del(dataset_json)


def main():
    scriptDir = os.path.dirname(os.path.realpath('__file__'))
    
    #datasets_folder_path = "/media/manuel/500GBHDD/Tesi/Datasets"                              #path to the folder that contains the datasets
    datasets_folder_path = "/home/manuel/Tesi/ACORDAR/Datasets"                              #path to the folder that contains the datasets

    error_log_file_path = os.path.join(scriptDir, 'logs/downloader_error_log.txt')             #path to the downloader error log file
    download_retry_log_path = os.path.join(scriptDir, 'logs/retry_download_log.txt')           #path to the dowloader retry error log file

    retryDownloads(datasets_folder_path, error_log_file_path, download_retry_log_path);


if __name__ == "__main__":
    main()