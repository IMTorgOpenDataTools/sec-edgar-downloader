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
from typing import ClassVar, Dict, List, Optional, Tuple, Union
from rapidfuzz import process, fuzz

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




class AccessionNumber:
    """The 'accession number' is SEC EDGAR's primary key.
    
    ex: Accession_Number('0001193125-15-118890')
    """
    
    def __init__(self, accession_number: str = None):
        if accession_number is None:
            self.short_cik: str = '1'
            self.year: str = '2001'
            self.annual_sequence: str = '000001'
            self.create_accession_number()
        self.parse_accession_number(accession_number)
        self.accession_number = accession_number
    
    def __repr__(self) -> str:
        return self.get_accession_number()

    def parse_accession_number(self, accession_number: str = None) -> dict[str, str]:
        """Split the 'accession number' into its constituent parts."""
        if accession_number is None:
            accession_number = self.accession_number
        split =  accession_number.split('-')
        self.short_cik = split[0].lstrip('0') 
        self.year = '20'+split[1]
        self.annual_sequence = split[2]
    
    def create_accession_number(self, short_cik: str = None, year: str = None, annual_sequence: str = None) -> str:
        """Create the 'accession number' in long-form (leading zeros)."""
        if (short_cik is None) or (year is None) or (annual_sequence is None):
            short_cik = self.short_cik
            year = self.year
            annual_sequence = self.annual_sequence
        zeros_to_add = 10 - len(short_cik)
        long_cik = str(0) * zeros_to_add + str(short_cik)
        self.accession_number = long_cik + '-' + str(year[2:4]) + '-' + str(annual_sequence)
        
    def get_parts_verification(self) -> str:
        """Get a verification that the numbers meet requirements."""
        check_cik = len(self.short_cik) < 10 and len(self.short_cik) > 1
        check_year = len(self.year) == 4 and self.year[0:2] == '20'
        check_annual_sequence = len(self.annual_sequence) == 6
        if check_cik and check_year and check_annual_sequence: 
            return 'Parts are correct'
        else:
            print( {'check_cik': check_cik, 'check_year': check_year, 'check_annual_sequence': check_annual_sequence} )
            return 'Fail - parts do not meet specs'

    def get_parts(self) -> dict[str, str, str]:
        """Get the parst of the 'accession number'."""
        return {'short_cik': self.short_cik, 'year': self.year, 'annual_sequence': self.annual_sequence}    
    
    def get_accession_number(self) -> str:
        """Get the 'accession number' in long-form (leading zeros)."""
        return self.accession_number

    def get_nodash_accession_number(self) -> str:
        """Get the 'accession number' in long-form (leading zeros) without dashes."""
        return self.accession_number.replace('-','')






class Filing:
    """The Filing functionality gets urls for the different files that may be available.
    
    sec - all filings across sec edgar site
    company - with unique cik
    filing - with unique accenssion number
    document - multiple formats for filing including :ixbrl, xbrl, txt, zip 

    Filing Date - date report is filed with SEC
    Period of Report - timespan the report describes
    """

    _url_sec_filing_search: str = "https://www.sec.gov/edgar/search/#/q={}"
    _url_company_search: str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&type={}"
    _url_filing_detail_page: str = 'https://www.sec.gov/Archives/edgar/data/{}/{}/{}-index.htm'
    _url_filing_document: str = 'https://www.sec.gov/Archives/edgar/data/{}/{}/{}'

    def __init__(self, short_cik:str, accession_number:AccessionNumber, file_type:str = None, file_date:str = None) -> None:
        self.short_cik = short_cik
        self.file_type = file_type
        self.file_date = file_date         
        self.filing_metadata = None
        self.accession_number = accession_number

        if self.accession_number == None:
            try:
                self.accession_number = self.get_accession_number()
            except:
                print('log: unable to get accession_number')
        try:
            self._get_filing_document_all_urls()
        except:
            print('log: unable to get all filing docs urls')

    def __repr__(self) -> str:
        rec = self.get_short_record()
        return rec.__repr__()

    @classmethod
    def from_accession_number(cls, accession_number: AccessionNumber) -> None:
        short_cik, file_type, file_date = cls._get_details(accession_number = accession_number)
        return cls(short_cik, file_type, file_date, accession_number)

    @classmethod
    def from_file_details(cls, short_cik:str, file_type:str, file_date:str) -> None:
        accession_number = cls._get_accession_number(short_cik, file_type, file_date)
        return cls(short_cik, accession_number, file_type, file_date)


    def _get_details(accession_number: AccessionNumber) -> Tuple[str, str, str]:
        """accession to cik,file_type,year) to get the actual data, ex:0000072971-20-000230
        __url_sec_filing_search = "https://www.sec.gov/edgar/search/#/q={}&dateRange=all"
        """
        url = 'https://efts.sec.gov/LATEST/search-index'
        payload = {"q":accession_number.accession_number,"dateRange":"all","startdt":"2001-01-01","enddt":"2022-02-22"}
        edgar_resp = requests.post(url, data=json.dumps(payload))
        edgar_resp.raise_for_status()
        time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
        edgar_list = edgar_resp.json()['hits']['hits']

        if len(edgar_list) > 1:
            print('log: multiple results match description.  taking the first available record:')
            print(edgar_list)
        
        short_cik = edgar_list[0]['_source']['ciks'][0].lstrip('0')     #'51143'
        file_type = edgar_list[0]['_source']['form']                    #'10-Q'
        file_date =  edgar_list[0]['_source']['file_date']              #'2021-01-29'
        return short_cik, file_type, file_date


    def _get_accession_number(short_cik:str, file_type:str, file_date:str) -> AccessionNumber:
        """Prepare minimum info to access data by taking most-recent filing for a date.

        :param cik
        :param file_type
        :param file_date
        :return AccessionNumber(accno)

        Usage::

        """
        headers = {
                "User-Agent": generate_random_user_agent(),
                "From": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate"
            }
        _url_company_search: str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&type={}"
        filled_url = _url_company_search.format(short_cik, file_type)
        edgar_resp = requests.get(filled_url, headers=headers)
        edgar_resp.raise_for_status()
        time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
        edgar_str = edgar_resp.text

        soup = BeautifulSoup(edgar_str, 'html.parser')
        tbl = soup.find_all('table')[2]
        df = pd.read_html(tbl.prettify())[0]
        
        df['Filing Date'] = pd.to_datetime(df['Filing Date'])
        row = df.loc[  (df['Filing Date'] == file_date)  ]

        item = row['Description']
        accno = str(item.values).split('Acc-no: ')[1].split('\\')[0]
        return AccessionNumber(accno)


    def _get_filing_document_all_urls(self) -> None:
        """Get the document's download url
        
        :param acc_no
        :param short_cik
        :return url
        """
        headers = {
                "User-Agent": generate_random_user_agent(),
                "From": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate"
            }
        print(self.accession_number.get_accession_number())
        acc_no_noformat = self.accession_number.get_nodash_accession_number()
        #'https://www.sec.gov/Archives/edgar/data/{short_cik}/{acc_no_noformat}/{acc_no}-index.htm'
        filled_url = self._url_filing_detail_page.format(self.short_cik, acc_no_noformat, str(self.accession_number))

        edgar_resp = requests.get(filled_url, headers=headers)
        edgar_resp.raise_for_status()
        time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
        edgar_str = edgar_resp.text
        soup = BeautifulSoup(edgar_str, 'html.parser')

        #ticker = 
        file_type = soup.find('div', attrs={'class':'companyInfo'}).find(text='Type: ').find_next_sibling('strong').text
        file_date = soup.find('div', text="Filing Date", attrs={'class': 'infoHead'}).find_next_sibling('div', attrs={'class': 'info'}).text
        self.file_type = file_type if self.file_type == None else self.file_type
        self.file_date = datetime.strptime(file_date, '%Y-%m-%d') if self.file_date == None else self.file_date

        tbls = soup.find_all('table')
        df = pd.DataFrame()
        for tbl in tbls:
            tmp = pd.read_html(tbl.prettify())[0]
            df = pd.concat([df, tmp], ignore_index=True)
        newdf = df

        href = []
        for tbl in tbls:
            for a in tbl.find_all('a', href=True):
                href.append(a['href'])
        href[0] = href[0].split('/ix?doc=')[1] if '/ix?doc=' in href[0] else href[0]
        extension = [ref.split('.')[1] for ref in href]
        df['URL'] = href
        df['Extension'] = extension
        #df = newdf.dropna(subset=['Description'])

        tmp_list = list(df.itertuples(name='Row', index=False))
        tmp_documents = [ DocumentMetadata(
                            Seq=rec.Seq,
                            Description=rec.Description,
                            Document=rec.Document,
                            Type=rec.Type,
                            Size=rec.Size,
                            URL=rec.URL,
                            Extension=rec.Extension,
                            FS_Location=''
                            ) for rec in tmp_list ]

        base_url = 'https://www.sec.gov'
        url_ixbrl = base_url + df[df['Type'].isin(SUPPORTED_FILINGS)]['URL'].values[0] if df[df['Type'].isin(SUPPORTED_FILINGS)].shape[0] > 0 else None
        url_xbrl = base_url + df[df['Description'].str.contains('INSTANCE', na=False)]['URL'].values[0] if df[df['Description'].str.contains('INSTANCE', na=False)].shape[0] > 0 else None 
        url_text = base_url + df[df['Extension'].str.contains('txt', na=False)]['URL'].values[0] if df[df['Extension'].str.contains('txt', na=False)].shape[0] > 0 else None
        url_zip = self._url_filing_document.format(self.short_cik, acc_no_noformat, str(self.accession_number)+'-xbrl.zip')
        url_xlsx = self._url_filing_document.format(self.short_cik, acc_no_noformat, 'Financial_Report.xlsx')
        url_exhibit = base_url + df[df['Type'].isin(['99.1'])]['URL'].values[0] if df[df['Type'].isin(['99.1'])].shape[0] > 0 else None

        self.filing_metadata = FilingMetadata(
            cik = self.short_cik,
            ticker = '',
            accession_number = self.accession_number,
            document_metadata_list = tmp_documents,
            filing_details_filename = '',
            full_submission_url = url_text,
            filing_details_url = url_ixbrl,
                
            filing_detail_page_url = filled_url,
            xlsx_financial_report_url = url_xlsx,
            html_exhibits_url = url_exhibit,
            xbrl_instance_doc_url = url_xbrl,
            zip_compressed_file_url = url_zip
            )

    def set_accession_number(self, accession_number: str) -> None:
        self.accession_number = AccessionNumber(accession_number)
        pass


    def get_short_record(self):
        """Convert some Filing data into record for ingest to dataframe"""
        rec = None
        if self.filing_metadata:
            asdict = self.filing_metadata._asdict()
            rec = {k:v for k,v in asdict.items() if (k != 'document_metadata_list' and 'url' not in k)}
            rec['Type'] = self.file_type
            rec['file_date'] = self.file_date
            rec['document_metadata_list'] = list(set([doc.Type for doc in asdict['document_metadata_list']]))
        return rec


    def get_complete_record(self):
        """Convert all Filing data into record for ingest to dataframe"""
        rec = None
        if self.filing_metadata:
            asdict = self.filing_metadata._asdict()
            rec = {k:v for k,v in asdict.items() }
            rec['Type'] = self.file_type
            rec['file_date'] = self.file_date
        return rec






class FilingStorage:
    """Functionality for maintaining a list of Filings as a binary file
    in a directory.
    
    param: FilingList
    param: dir_path
    """

    _file_name = 'filing_storage.pickle'
    
    def __init__(self, dir_path:Path):
        self.file_path = Path.joinpath(dir_path, self._file_name) 
        self.__FilingList:List[FilingMetadata] = []
        
        if Path.exists(self.file_path) and Path.is_file(self.file_path):
            self.load_from_pickle()
        else:
            self.dump_to_pickle()
            print('log: created file for storing Filings list')

    def __repr__(self):
        cnt = len(self.get_list())
        return f"FilingStorage with {cnt} files"

    def dump_to_pickle(self):
        """Dumps so that pickled data can be loaded with Python 3.4 or newer"""
        with open(self.file_path, 'wb') as File:
            pickle.dump(self.__FilingList, File, protocol=4)
        print('log: updated filing storage')


    def load_from_pickle(self):
        """Load the pickle file with Python 3.4 or newer"""
        with open(self.file_path, 'rb') as File:
            self.__FilingList = pickle.load(File)
        print(f'log: loaded Filing list from file: {self.file_path}')


    def add_new_list(self, new_list:List[FilingMetadata]):
        if type(new_list) == list:
            unique_list = list(set(new_list))
            self.__FilingList.extend(unique_list)   
        self.dump_to_pickle()


    def get_list(self):
        return self.__FilingList


    def get_dataframe(self, mode='short'):
        match mode:
            case 'short':
                list_of_dicts = [rec.get_short_record() for rec in self.__FilingList if isinstance(rec.get_short_record(), dict)]
            case 'long':
                list_of_dicts = [rec.get_complete_record() for rec in self.__FilingList]
        df = pd.DataFrame(list_of_dicts)
        return df


    def set_list(self, new_list):
        if len(new_list) > 0 and type(new_list) == List[FilingMetadata] and len(self.__FilingList) < 1:
            self.__FilingList = new_list






class Firm():
    """TODO:The Firm is root for all classes used.

    TODO: __repr__() method for listing
    """
    __ciks = set()
    
    def __init__(self, firm_name=None, ticker=None, cik=None):
        """Create populated Firm object, if object not previously created."""
        self._name = firm_name
        self._cik = cik
        self._ticker = ticker
        self._exchange = None

        #TODO: getters/setters
        self._report = None
        self._security = None
        self._management = None

        if self._name:
            try:
                result = self._get_info_from_name(firm_name)
            except:
                raise Exception("The firm is not available.")
            else:
                self._name = result['name']
                self._cik = result['cik']
                self._ticker = result['ticker']
                self._exchange = result['exchange']
                
        elif self._ticker:
            try:
                result = self._get_info_from_ticker(ticker)
            except:
                raise Exception("The ticker is not available.")
            else:
                self._name = result['name']
                self._cik = result['cik']
                self._ticker = result['ticker']
                self._exchange = result['exchange']
        else:
            pass
        if self._cik in Firm.__ciks:
            raise ValueError('firm previously created')
        elif self._cik == None:
            raise ValueError('firm not registered at sec: no cik is found')
        else:
            Firm.__ciks.add(self._cik)


    def __repr__(self) -> str:
        val = self.get_info(info='name') 
        return val if val != None else 'None'
            

    def __get_list_of_all_firms__(self):
        return [ o for o in gc.get_objects() if isinstance(o, Firm)]

    
    def get_info(self, info='all'):
        """Return all or specific firm information from attributes"""
        if self._cik:
            results = {'cik':self._cik, 'name': self._name, 'ticker': self._ticker, 'exchange': self._exchange}
        else:
            return None
        if info=='all':
            return results
        elif info in ['cik','name','ticker','exchange']:
            return results[info]


    def _get_info_from_name(self, firm_name):
        """Get firm info from lookup.

        Args:
            firm_name(str): closely-related firm name
            info: 'cik', 'name', 'ticker', 'exchange', or 'all'

        Returns:
            'all': {'cik', 'name', 'ticker', 'exchange', or 'all'}

        TODO: get additional info from `<browser> https://www.sec.gov/edgar/browse/?CIK=1084869`
        address, industry, etc. can be used for querying on firms
        TODO: add this as class-level data for use across all objects (pulled only once)
        """
        result = self.get_info()
        if result is None:
            url = 'https://www.sec.gov/files/company_tickers_exchange.json'
            resp = requests.get(url)
            json = resp.json()

            choices = [item[1] for item in json['data']]
            possible_names = process.extract(firm_name, choices, scorer=fuzz.WRatio, limit=3)
            print(f'Chose the first of these closely-related names: {possible_names}')
            result = [item for item in json['data'] if item[1]==possible_names[0][0]]

            rtn_item = {}
            for idx,key in enumerate(json['fields']):
                rtn_item[str(key)] = result[0][idx]
            return rtn_item
        

    def _get_info_from_ticker(self, ticker):
            url = 'https://www.sec.gov/files/company_tickers_exchange.json'
            resp = requests.get(url)
            json = resp.json()

            json['data']
            result = [item for item in json['data'] if item[2]==ticker]
            
            rtn_item = {}
            for idx,key in enumerate(json['fields']):
                rtn_item[str(key)] = result[0][idx]

            return rtn_item                  
        

    def get_reports_count(self):
        #TODO: check_reports_in_cache
        cnt = len(self._reports)
        return cnt


    def get_insider_transactions(self):
        #TODO: https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK=0001084869
        pass