"""
This script will help the indexing phase by cleaning the data that is extracted from the developed parsers.
The scope is to remove from the lists of literals, classes ecc. all the elements that are codes, numbers and 
other string that does not contains human readable text and thus are not so useful in a Lucene index
"""

import json
import os
import argparse
import logging
import re
from tqdm import tqdm

def clean_dataset_content(dataset_content_rdflibhr_path:str, dataset_content_rdflibhr_clean_path:str):
    dataset_content_rdflibhr_file = open(dataset_content_rdflibhr_path, "r", encoding="utf-8")
    dataset_content_rdflibhr = json.load(dataset_content_rdflibhr_file)
    dataset_content_rdflibhr_file.close()

    dataset_content_rdflibhr_clean = {
        "entities" : list(),  
        "literals" : list(), 
    }

    fields = ["entities", "literals"]

    #clean the fields
    for field in fields:
        for element in dataset_content_rdflibhr[field]:
            if " " in element:
                numbers = re.findall("\d+", element)
                
                if len(numbers) < 0.5 * len(element):

                    #clean from control characters
                    element = re.sub("\\n|\\r|  +", ' ', element)

                    #clean from html tags 
                    element = re.sub(r'<.*?>', ' ', element)

                    dataset_content_rdflibhr_clean[field].append(element)

    #save the new file
    dataset_content_rdflibhr_clean_file = open(dataset_content_rdflibhr_clean_path, "w", encoding="utf-8")
    json.dump(dataset_content_rdflibhr_clean, dataset_content_rdflibhr_clean_file, ensure_ascii=False, indent=4)
    dataset_content_rdflibhr_clean_file.close()


"""
@param: datasets_folder, path to the folder where all the datasets are stored
"""
def clean_datasets_contents_labels(datasets_folder:str):

    bar = tqdm(total = len(os.listdir(datasets_folder)))

    for dataset in os.scandir(datasets_folder):
        dataset_content_rdflibhr_path = dataset.path + "/dataset_content_rdflibhr.json"

        if os.path.exists(dataset_content_rdflibhr_path):
            dataset_content_rdflibhr_clean_path = dataset.path + "/dataset_content_rdflibhr_clean.json"
            clean_dataset_content(dataset_content_rdflibhr_path, dataset_content_rdflibhr_clean_path)

        bar.update(1)


if __name__ == "__main__":
    # read the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "datasets_folder", 
        type=str, 
        help="Absolute path to the folder where all the datasets are stored"
    )
    parser.add_argument(
        "parsing_strategy",
        type=str,
        help="Which parsing strategy dataset_content files must be cleaned (\"labels\" or \"standard\")",
    )
    args = parser.parse_args()        

    if args.parsing_strategy == "labels":
        clean_datasets_contents_labels(args.datasets_folder)

