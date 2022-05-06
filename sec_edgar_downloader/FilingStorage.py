import os
from pathlib import Path
from re import L
import copy

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
from collections import OrderedDict
#from ordered_set import OrderedSet
from rapidfuzz import process, fuzz

#from ._utils import generate_random_user_agent
from ._constants import (
    #FilingMetadata,
    DocumentMetadata,
    #get_number_of_unique_filings,
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
from . import UrlComponent as uc







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
        #self.__FilingSet:OrderedDict[FilingMetadata] = OrderedDict()
        self.__FilingSet:OrderedDict = OrderedDict()
        
        if Path.exists(self.file_path) and Path.is_file(self.file_path):
            self.load_from_pickle()
            self.sync_with_filesystem()
        else:
            self.dump_to_pickle()
            print('log: created file for storing Filings list')

    def __repr__(self):
        cnt = len(self.__FilingSet)
        return f"FilingStorage with {cnt} filing records"

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

    '''
    def check_record_exists(self,
                            filing,
                            ticker_or_cik,
                            after):
        """TODO:check records before spending time getting there urls."""
        cnt = len(self.get_all_records())
        if cnt < 0:
            df = self.get_dataframe(mode='file')
            sel1 = df[df['file_type']==filing & df['ticker']==ticker_or_cik & df['file_date']>=after]
            if sel1.shape[0] > 0:
                pass
        pass'''


    def add_record(self, record:uc.Filing = None, rec_lst:List[uc.Filing] = None):
        """Add a single record or a list of records"""
        def add_rec(record):
            key = record.create_key()
            self.__FilingSet[key] = record
            return None

        if type(record) == uc.Filing and record.short_cik and record.accession_number:
            add_rec(record)
            print(record.short_cik)
        elif type(rec_lst) == list and len(rec_lst) > 0:
            result = [add_rec(record) for record in rec_lst]
            print(f"Added {len(result)} records.")
        elif record == None:
            print(f"No record(s) provided.")
            return None
        else:
            raise TypeError
        self.dump_to_pickle()
        return None


    def get_all_records(self, mode='file'):
        if mode == 'file':
            return self.__FilingSet
        elif mode == 'document':
            dict_of_docs = {}
            for k,v in self.__FilingSet.items():
                for doc in v.document_metadata_list:
                    doc_key = k + '|' + str(doc.Seq)
                    dict_of_docs[doc_key] = doc
            return dict_of_docs
        else:
            raise TypeError
        return None

    
    def get_record(self, idx_or_key):
        """Find a record in the OrderedDict by index or key."""
        if type(idx_or_key) == str:
            if idx_or_key in self.__FilingSet.keys():
                result = self.__FilingSet[idx_or_key]
                return result
            else:
                TypeError
        elif type(idx_or_key) == int:
            if idx_or_key < len( self.__FilingSet.items() ):
                result = list(self.__FilingSet.items())[idx_or_key]
                return result
            else:
                TypeError
        else:
            TypeError


    def get_document_in_record(self, idx_or_list):
        """Return the Doc(ument) from within records."""
        dict_of_docs = {}
        for k,v in self.__FilingSet.items():
            for doc in v.document_metadata_list:
                doc_key = k + '|' + str(doc.Seq)
                dict_of_docs[doc_key] = doc
        lst = list(dict_of_docs.items())
        if type(idx_or_list)==int:
            return lst[idx_or_list]
        elif type(idx_or_list)==list:
            return [lst[idx] for idx in idx_or_list]
        else:
            raise TypeError


    def modify_record(self, orig_record, new_record):
        """This is necessary because you cannot set value by index."""
        key = orig_record.create_key()
        self.__FilingSet.pop(key)
        self.__FilingSet[key] = new_record
        return None

    
    def modify_document_in_record(self, file_key, orig_document, new_document):
        """Modify a record's document with a new one."""
        orig_rec = self.__FilingSet[file_key]
        new_rec = copy.deepcopy( orig_rec )
        idx = new_rec.document_metadata_list.index(orig_document)
        if idx!=None and type(new_document) == DocumentMetadata:
            new_rec.document_metadata_list[idx] = new_document
            self.__FilingSet.pop(file_key)
            self.__FilingSet[file_key] = new_rec
        else:
            raise TypeError
        return None


    def get_list(self):
        """Return a list of all records."""
        return list(self.__FilingSet.values())

    
    def get_dataframe(self, mode='file'):
        """Return a dataframe of all records."""
        match mode:
            case 'file':
                list_of_dicts = [rec.get_file_record() for rec in list(self.__FilingSet.values()) if isinstance(rec.get_file_record(), dict)]
            case 'document':
                list_of_dicts = []
                for rec in list(self.__FilingSet.values()):
                    list_of_dicts.extend( rec.get_document_record_list() )
        df = pd.DataFrame(list_of_dicts)
        return df

    
    def sync_with_filesystem(self):
        """Uses the downloaded files to determine correctness of filings' FS_Location.
        This is useful when the FilingStorage (`self.__FilingSet`) becomes corrupt.

        TODO: add functionality to populate records, if only the downloaded documents are present.
        """

        def get_all_file_path(dir):
            #get all files
            walk = [file for file in os.fwalk(dir)]
            max_depth = max([file[3] for file in walk])
            docs = {}
            for file in walk:
                if file[3]==max_depth:
                    for doc in file[2]:
                        docs.update({doc : Path(file[0]) / doc})
            return docs

        cnt = 0
        dir = self.file_path.parent
        downloaded_docs = get_all_file_path(dir)
        #get all documents
        recs = copy.deepcopy( list(self.get_all_records().items()) )
        for rec in recs:
            file_key = rec[0]
            for doc in rec[1].document_metadata_list:
                doc_name = doc.Document.split()[0]
                if doc_name in downloaded_docs.keys():
                    new_doc = doc._replace(FS_Location = downloaded_docs[doc_name])
                    self.modify_document_in_record(file_key, doc, new_doc)
                    cnt += 1
        self.dump_to_pickle()
        print(f'There were {cnt} document FS_Location synced with filesystem')
        return None



    '''
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
        '''