import os
from pathlib import Path

import requests
import json
import time
import pickle
from datetime import datetime
import gc

from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from typing import ClassVar, Dict, List, OrderedDict, Set, Optional, Tuple, Union
from ordered_set import OrderedSet
from rapidfuzz import process, fuzz

import os
import subprocess
import mmap
import re

#from ._utils import generate_random_user_agent
from ._constants import (
    FilingMetadata,
    DocumentMetadata,
    get_number_of_unique_filings,
    generate_random_user_agent,
    is_cik,
    DATE_FORMAT_TOKENS,
    FILING_DETAILS_FILENAME_STEM,
    FILING_FULL_SUBMISSION_FILENAME,
    MAX_RETRIES,
    ROOT_SAVE_FOLDER_NAME,
    SEC_EDGAR_ARCHIVES_BASE_URL,
    SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL,
    SEC_EDGAR_SEARCH_API_ENDPOINT,
    SUPPORTED_FILINGS
)







class FilingStorage:
    """Functionality for maintaining a one-to-one mapping with all SEC
    EDGAR filings / documents.  This mapping includes the associated
    EDGAR urls, as well as the documents downloaded to the file system.
    
    param: FilingList
    param: dir_path
    """

    _file_name = 'filing_storage.pickle'
    
    def __init__(self, dir_path:Path):
        self.file_path = Path.joinpath(dir_path, self._file_name) 
        self.__FilingSet:OrderedSet[FilingMetadata] = []
        
        if Path.exists(self.file_path) and Path.is_file(self.file_path):
            self.load_from_pickle()
        else:
            self.dump_to_pickle()
            print('log: created file for storing Filings list')

    def __repr__(self):
        cnt = len(self.get_all_records())
        return f"FilingStorage with {cnt} files"

    def dump_to_pickle(self):
        """Dumps so that pickled data can be loaded with Python 3.4 or newer"""
        with open(self.file_path, 'wb') as File:
            pickle.dump(self.__FilingSet, File, protocol=4)
        print('log: updated filing storage')


    def load_from_pickle(self):
        """Load the pickle file with Python 3.4 or newer"""
        with open(self.file_path, 'rb') as File:
            self.__FilingSet = pickle.load(File) 
        print(f'log: loaded Filing list from file: {self.file_path}')


    def add_record(self, record:FilingMetadata):
        """Add a single record"""
        if type(record) == FilingMetadata:
            self.__FilingSet.add(record)  
        self.dump_to_pickle()


    def add_record_list(self, record_list:List[FilingMetadata]):
        """Add a list of records.  Preferable because of latency in IO operation."""
        if type(record_list) == List[FilingMetadata]:
            ordr_set = OrderedSet(record_list)
            self.__FilingSet.update(ordr_set)  
        self.dump_to_pickle()

    def get_all_records(self):
        """Return an OrderedSet of all records."""
        return self.__FilingSet


    def get_dataframe(self, mode='file'):
        """Return a dataframe of all records."""
        match mode:
            case 'file':
                list_of_dicts = [rec.get_file_record() for rec in self.__FilingSet if isinstance(rec.get_file_record(), dict)]
            case 'document':
                list_of_dicts = []
                for rec in self.__FilingSet:
                    list_of_dicts.extend( rec.get_document_record_list() )
        df = pd.DataFrame(list_of_dicts)
        return df


    def sync_with_filesystem(self):
        """Uses the downloaded files to determine correctness."""
        pass


    def search_filings(self, idx, file_type):
        """TODO: this is fucked up"""
        result_list = []
        for file in dl.filing_storage.get_list():
            documents = file.filing_metadata.document_metadata_list
            tmp_doc = [doc for doc in documents if (type(doc.Type)==str)]
            rtn_doc = [doc for doc in tmp_doc if file_type in doc.Type]

            for doc in rtn_doc:
                save_path = (
                     dl.download_folder
                     / 'sec-edgar-filings'
                     / file.short_cik
                     / file.file_type
                     / file.accession_number.__str__()
                     / doc.Document
                     )
                FS_Location = save_path
                result_list.append( (rtn_doc, FS_Location) )
        return result_list


    def load_documents(self, documents):
        """Load all staged documents into memory."""
        return_list = []
        for doc in documents:
            txt = doc[1].read_bytes()
            soup = BeautifulSoup(txt, 'lxml')
            return_list.append(soup)
        return return_list


    def search_docs_for_terms(search_list = [r'allowance', r'credit loss'], ignore_case=True, file_ext='htm'):
        """Search through files for specific regex term(s) and obtain snippet of surrounding text.

        param: search_list
        return: {file, index, text}
        """
        search_term = '|'.join(search_list)            #gives this r'allowance|credit|loss'
        regex = bytes(search_term, encoding='utf8')    #equivalent to regex = rb"\bcredit|loss\b"
        START = 50
        END = 50

        re_term = re.compile(regex)
        dir_path = str( dl.download_folder / 'sec-edgar-filings')
        total_files = sum([len(files) for r, d, files in os.walk(dir_path)])

        cmd = ['grep','-Ei', search_term, '-rnwl', dir_path] if ignore_case else ['grep','-E', search_term, '-rnwl', dir_path]
        hits = subprocess.run(cmd, capture_output=True)
        files = hits.stdout.decode('utf-8').split('\n')
        files_not_empty = [file for file in files if (file != '' and file_ext in file.split('.')[1])]
        print(f'log: number of files matching criteria: {len(files_not_empty)} of {total_files}')

        results = []
        if len(files_not_empty) > 0:
            for file in files_not_empty:
                with open(file) as f:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmap_obj:
                        for term in re_term.finditer(mmap_obj):

                            start_idx = term.start() - START if term.start() - START >= 0 else 0
                            end_idx = term.end() + END if term.end() + END < len(mmap_obj) else len(mmap_obj)
                            text = mmap_obj[ start_idx : end_idx].decode('utf8')
                            start_format = START
                            end_format = len(text) - END
                            rec = {'file':file, 
                                   'index': term.start(), 
                                   'text': text, 
                                   'start_format':start_format, 
                                   'end_format':end_format 
                                  }
                            results.append( rec )

            print(f'log: number of lines matching criteria: {len(results)}')
            return results
        else:
            return None