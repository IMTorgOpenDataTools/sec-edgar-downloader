#!/usr/bin/env python3
"""
Components used in SEC EDGAR's url endpoints.

Each SEC submision Filing has a unique AccessionNumber and is filed by a Firm with
a unique CIK.  Each Filing has metadata and multiple documents.  Each document has
associated DocumentMetadata.

Classes:
    AccessionNumber
    Filing
    Firm

"""

__author__ = "Jason Beach"
__version__ = "0.1.0"
__license__ = "MIT"


import requests
import json
import time
from datetime import datetime

from bs4 import BeautifulSoup
import pandas as pd
from rapidfuzz import process, fuzz

from ._constants import (
    DocumentMetadata,
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
    SEC_EDGAR_CURRENT_SEARCH_BASE_URL,
    SUPPORTED_FILINGS
)





class AccessionNumber:
    """The 'accession number' is SEC EDGAR's primary key.
    Objects of this class enable creation from (or decomposition of) its multiple 
    parts (or different accession number formats).  Its three components include:
    * short_cik
    * year
    * annual_sequence

    Usage::
        >>> Accession_Number('0001193125-15-118890')
        >>> Accession_Number().get_parts() == {'short_cik': '1', 'year': '2001', 'annual_sequence': '000001'}
    """
    
    def __init__(self, accession_number: str = None):
        """Create an object of type AccessionNumber.    
        :param accession_number(str)
        or
        :param short_cik(str)
        :param year(str)
        :param annual_sequence(str)

        :return None
        """
        if accession_number is None:
            self.short_cik: str = '999999999'
            self.year: str = '2099'
            self.annual_sequence: str = '999999'
            self.create_from_components()
        self.parse_accession_number(accession_number)
        if self.verify_components_meet_requirements():
            self.accession_number: str = accession_number
        else:
            raise TypeError
        pass
    
    def __repr__(self) -> str:
        return self.get_accession_number_string()

    def parse_accession_number(self, accession_number: str = None) -> dict[str, str]:
        """Split the 'accession number' into its constituent parts."""
        if accession_number is None:
            accession_number = self.accession_number
        split = accession_number.split('-')
        self.short_cik = split[0].lstrip('0') 
        self.year = '20'+split[1]
        self.annual_sequence = split[2]
    
    def create_from_components(self, short_cik: str = None, year: str = None, annual_sequence: str = None) -> str:
        """Given the three component parts, create the 'accession number' in long-form (leading zeros)."""
        if (short_cik is None) or (year is None) or (annual_sequence is None):
            short_cik = self.short_cik
            year = self.year
            annual_sequence = self.annual_sequence
        zeros_to_add = 10 - len(short_cik)
        long_cik = str(0) * zeros_to_add + str(short_cik)
        if self.verify_components_meet_requirements() == False:
            raise TypeError
        self.accession_number = long_cik + '-' + str(year[2:4]) + '-' + str(annual_sequence)
        
    def verify_components_meet_requirements(self) -> str:
        """Verification that the component numbers meet requirements."""
        check_cik = type( self.short_cik)==str and len(self.short_cik) < 10 and len(self.short_cik) > 1
        check_year = type( self.year)==str and len(self.year) == 4 and self.year[0:2] == '20'
        check_annual_sequence = type( self.annual_sequence)==str and len(self.annual_sequence) == 6
        if check_cik and check_year and check_annual_sequence: 
            return True   #Parts are correct
        else:
            print( {'check_cik': check_cik, 'check_year': check_year, 'check_annual_sequence': check_annual_sequence} )
            return False    #Fail - parts do not meet specs

    def get_components(self) -> dict[str, str]:
        """Get the parts of the 'accession number'.
        :return dict(str,str) - keys: short_cik, year, annual_sequence.
        """
        return {'short_cik': self.short_cik, 'year': self.year, 'annual_sequence': self.annual_sequence}    
    
    def get_accession_number_string(self) -> str:
        """Get the 'accession number' in long-form (leading zeros) with dashes."""
        return str(self.accession_number)

    def get_nodash_accession_number(self) -> str:
        """Get the 'accession number' in long-form (leading zeros) without dashes."""
        return self.accession_number.replace('-','')






class Filing:
    """The Filing object maintains all metadata concerning a specific firm's Filing
    submission to SEC EDGAR and all documents' metadata for the Filing.  It includes
    functionality to get data from the primary SEC EDGAR filing urls.

    _NOTE_: calls to SEC EDGAR API are made on initialization 
    
    Entities referenced include:
    * sec - all filings across sec edgar site
    * firm - with unique cik
    * filing - with unique accenssion number
    * documents - filings have multiple documents with formats including :ixbrl, xbrl, txt, zip 
    * Filing Date - date report is filed with SEC
    * Period of Report - timespan the report describes

    Usage::
        >>> File = Filing(short_cik, accession_number)
    """

    #_url_sec_filing_search: str = "https://www.sec.gov/edgar/search/#/q={}"
    _url_sec_filing_search: str = SEC_EDGAR_SEARCH_API_ENDPOINT    #"https://efts.sec.gov/LATEST/search-index"
    _url_company_search: str = SEC_EDGAR_CURRENT_SEARCH_BASE_URL+'/browse-edgar?action=getcompany&CIK={}&type={}'     "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&type={}"
    _url_filing_detail_page: str = SEC_EDGAR_ARCHIVES_BASE_URL+'/{}/{}/{}-index.htm'   #'https://www.sec.gov/Archives/edgar/data/{}/{}/{}-index.htm'
    _url_filing_document: str =  SEC_EDGAR_ARCHIVES_BASE_URL+'/{}/{}/{}'    #'https://www.sec.gov/Archives/edgar/data/{}/{}/{}'

    def __init__(self, accession_number:AccessionNumber = None, short_cik:str = None, file_type:str = None, file_date:str = None) -> None:
        """Create an object of type Filing from the accession_number for the file details.
        :param accession_number(AccessionNumber)
        or
        :param short_cik(str)
        :param file_type(str)
        :param file_date(str)
        
        :return None
        """
        #param
        if accession_number and short_cik and file_type and file_date:
            if type(accession_number)==AccessionNumber and type(short_cik)==str and type(file_type)==str and type(file_date)==str:
                self.accession_number: AccessionNumber = accession_number
                self.short_cik: str = str(short_cik)
                self.file_type: str = str(file_type)
                self.file_date: str = str(file_date)
        elif accession_number:
            if type(accession_number)==AccessionNumber:
                self.accession_number: AccessionNumber = accession_number
                self.short_cik: str = str(accession_number.get_components()['short_cik'])
                self.file_type: str = None
                self.file_date: str = None
                rslt = self._get_filing_details()
                #if self.short_cik == rslt[0]:    TODO:check if this is correct
                if True:
                    self.short_cik = rslt[0]
                    self.file_type = rslt[1]
                    self.file_date = rslt[2]
            else:
                raise TypeError
        elif short_cik and file_type and file_date:
            if type(short_cik)==str and type(file_type)==str and type(file_date)==str:
                self.short_cik: str = str(short_cik)
                self.file_type: str = str(file_type)
                self.file_date: str = str(file_date)
                self.accession_number = self._get_accession_number()
            else:
                raise TypeError
        else:
            raise TypeError         
        
        #provision
        self.report_date: str = None
        self.document_metadata_list: list = []

        self.filing_details_filename: str = None
        self.full_submission_url: str = None
        self.filing_details_url: str = None
        self.filing_detail_page_url: str = None
        self.xlsx_financial_report_url: str = None
        self.html_exhibits_url: str = None
        self.xbrl_instance_doc_url: str = None
        self.zip_compressed_file_url: str = None

        self._get_filing_documents()
        pass


    def __repr__(self) -> str:
        rec = self.get_file_record()
        return rec.__repr__()


    def _get_filing_details(self) -> tuple[str, str, str]:
        """Call to API with accession_number to get filing's data details (short_cik, file_type, file_date).
        
        :param accession_number(AccessionNumber)
        
        :return file_detail: tuple(short_cik(str), file_type(str), file_date(str))
        
        """
        #call to api
        enddt = datetime.now().strftime('%Y-%m-%d')
        headers = {
                "User-Agent": generate_random_user_agent(),
                "From": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate"
            }
        payload = {"q":self.accession_number.accession_number,
                   "dateRange":"all",
                   "startdt":"2001-01-01",
                   "enddt":enddt}
        edgar_resp = requests.post(url=self._url_sec_filing_search, 
                                   data=json.dumps(payload), 
                                   headers=headers
                                   )
        edgar_resp.raise_for_status()
        time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
        edgar_list = edgar_resp.json()['hits']['hits']

        #parse response
        if len(edgar_list) > 1:
            print('log: multiple results match description.  taking the first available record:')
            print(edgar_list)
        
        short_cik = edgar_list[0]['_source']['ciks'][0].lstrip('0')     #'51143'
        file_type = edgar_list[0]['_source']['form']                    #'10-Q'
        file_date =  edgar_list[0]['_source']['file_date']              #'2021-01-29'
        file_details = short_cik, file_type, file_date
        return file_details


    def _get_accession_number(self) -> AccessionNumber:
        """Call to API with minimum information to get most-recent filing accession_number for a date.

        :param cik
        :param file_type
        :param file_date

        :return accenssion_number(AccessionNumber)
        """
        #call to api
        short_cik = self.short_cik
        file_type = self.file_type
        file_date = self.file_date
        filled_url = self._url_company_search.format(short_cik, file_type)
        headers = {
                "User-Agent": generate_random_user_agent(),
                "From": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate"
            }
        edgar_resp = requests.get(url=filled_url, 
                                  headers=headers
                                  )
        edgar_resp.raise_for_status()
        time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
        edgar_str = edgar_resp.text

        #parse response
        soup = BeautifulSoup(edgar_str, 'html.parser')
        tbl = soup.find_all('table')[2]
        df = pd.read_html(tbl.prettify())[0]
        df['Filing Date'] = pd.to_datetime(df['Filing Date'])
        row = df.loc[  (df['Filing Date'] == file_date)  ]

        item = row['Description']
        acc_no = str(item.values).split('Acc-no: ')[1].split('\\')[0]
        accenssion_number = AccessionNumber(acc_no)
        return accenssion_number


    def _get_filing_documents(self) -> bool:
        """Call to API to get all documents' metadata for the Filing.  This populates Filing 
        attributes, directly, and only returns True if the urls and connections are correct.

        :param short_cik    
        :param acc_no
        
        :return bool
        """
        #call to api
        short_cik = self.short_cik
        accession_number = self.accession_number
        acc_no_noformat = self.accession_number.get_nodash_accession_number()
        filled_url = self._url_filing_detail_page.format(short_cik, acc_no_noformat, str(accession_number))
        headers = {
                "User-Agent": generate_random_user_agent(),
                "From": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate"
            }
        edgar_resp = requests.get(filled_url, headers=headers)
        edgar_resp.raise_for_status()
        time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
        edgar_str = edgar_resp.text

        #parse response for file info
        soup = BeautifulSoup(edgar_str, 'html.parser')
        file_type = soup.find('div', attrs={'class':'companyInfo'}).find(string='Type: ').find_next_sibling('strong').text
        file_date = soup.find('div', string="Filing Date", attrs={'class': 'infoHead'}).find_next_sibling('div', attrs={'class': 'info'}).text
        report_date = soup.find('div', string="Period of Report", attrs={'class': 'infoHead'}).find_next_sibling('div', attrs={'class': 'info'}).text
        self.file_type = file_type if self.file_type == None else self.file_type
        self.file_date = datetime.strptime(file_date, '%Y-%m-%d') if self.file_date == None else self.file_date
        self.report_date = datetime.strptime(report_date, '%Y-%m-%d') if self.report_date == None else self.report_date

        #parse response for url info
        tbls = soup.find_all('table')
        df = pd.DataFrame()
        for tbl in tbls:
            tmp = pd.read_html(tbl.prettify())[0]
            df = pd.concat([df, tmp], ignore_index=True)
        href = []
        for tbl in tbls:
            for a in tbl.find_all('a', href=True):
                href.append(a['href'])
        href[0] = href[0].split('/ix?doc=')[1] if '/ix?doc=' in href[0] else href[0]
        extension = [ref.split('.')[1] for ref in href]
        df['URL'] = href
        df['Extension'] = extension

        #create DocumentMetadata for all of the Filings' documents
        tmp_list = list(df.itertuples(name='Row', index=False))
        tmp_documents = [ DocumentMetadata(
                            Seq=rec.Seq,
                            Description=rec.Description,
                            Document=rec.Document,
                            Type=rec.Type,
                            Size=rec.Size,
                            URL=rec.URL,
                            Extension=rec.Extension,
                            FS_Location=None,
                            Report_date=report_date,
                            ) for rec in tmp_list ]

        #populate Filings' document url
        base_url = 'https://www.sec.gov'
        url_ixbrl = base_url + df[df['Type'].isin(SUPPORTED_FILINGS)]['URL'].values[0] if df[df['Type'].isin(SUPPORTED_FILINGS)].shape[0] > 0 else None
        url_xbrl = base_url + df[df['Description'].str.contains('INSTANCE', na=False)]['URL'].values[0] if df[df['Description'].str.contains('INSTANCE', na=False)].shape[0] > 0 else None 
        url_text = base_url + df[df['Extension'].str.contains('txt', na=False)]['URL'].values[0] if df[df['Extension'].str.contains('txt', na=False)].shape[0] > 0 else None
        url_zip = self._url_filing_document.format(self.short_cik, acc_no_noformat, str(self.accession_number)+'-xbrl.zip')
        url_xlsx = self._url_filing_document.format(self.short_cik, acc_no_noformat, 'Financial_Report.xlsx')
        url_exhibit = base_url + df[df['Type'].isin(['99.1'])]['URL'].values[0] if df[df['Type'].isin(['99.1'])].shape[0] > 0 else None

        self.document_metadata_list.extend( tmp_documents ),
        self.filing_details_filename = None,
        self.full_submission_url = url_text,
        self.filing_details_url = url_ixbrl,
                
        self.filing_detail_page_url = filled_url,
        self.xlsx_financial_report_url = url_xlsx,
        self.html_exhibits_url = url_exhibit,
        self.xbrl_instance_doc_url = url_xbrl,
        self.zip_compressed_file_url = url_zip

        print(f'loaded filing: {self.accession_number.get_accession_number_string()}')
        return True


    def create_key(self, short_cik: str = None, accession_number: AccessionNumber = None) -> str:
        """Create unique Filing key for use with management in dict."""
        if short_cik == None or accession_number == None:
            short_cik = self.short_cik
            accession_number = self.accession_number
        return short_cik + '|' + accession_number.get_accession_number_string()

    
    def get_file_record(self) -> str:
        """Convert some Filing data into (dict) record for display and query."""
        rec = None
        asdict = self.__dict__
        rec = {k:v for k,v in asdict.items() if (k != 'document_metadata_list' and 'url' not in k)}
        rec['file_type'] = self.file_type
        rec['file_date'] = self.file_date
        rec['report_date'] = self.report_date
        rec['doc_types'] = list(set([doc.Type for doc in asdict['document_metadata_list']]))
        return rec

    
    def get_document_record_list(self) -> dict:
        """Convert all Filing data into record for ingest to dataframe."""
        result = []
        for doc in self.document_metadata_list:
            asdict = doc._asdict()
            rec = {k:v for k,v in asdict.items()}
            rec['short_cik'] = self.short_cik
            rec['accession_number'] = self.accession_number
            rec['file_type'] = self.file_type
            rec['file_date'] = self.file_date
            rec['report_date'] = self.report_date
            rec['yr-month'] = f'{self.report_date.year}-{self.report_date.month}'
            result.append(rec)
        return result
        





class Firm():
    """Objects of class Firm maintain data for firms that submit Filings.
    Firm also maintains a class-level variables with all ciks.

    _NOTE_: calls to SEC EDGAR API are made on initialization  
    
    """
    __initialized_ciks = set()
    __sec_registry = None
    
    def __init__(self, cik:str=None, ticker:str=None, name:str=None) -> None:
        """Create populated Firm object, if object not previously created.
        Object must be instantiated with one of the three parameters that
        uniquely identifies an SEC firm.  Parameters are ordered by their
        preferred accuracy in identification.
        """
        #get firm info
        selection = []
        for param in ['name', 'ticker', 'cik']:
            value = locals()[param]
            if param in list(locals().keys()) and value!=None and type(value)==str:
                selection.append( (param,value.upper()) )
        if len(selection)<1:
            raise TypeError
        k,v = selection[0]
        firm_info_dict = self.get_firm_info_from_sec_registry(param=k, value=v)

        #provision
        self._cik = str(firm_info_dict['cik'])
        self._ticker = firm_info_dict['ticker']
        self._name = firm_info_dict['name']
        self._exchange = firm_info_dict['exchange']

        #TODO: getters/setters
        self._report = None
        self._security = None
        self._management = None

        #add firm to records
        firm_previously_initialized = self.check_firm_for_previous_initialize_update_if_not(cik=firm_info_dict['cik'])
        if firm_previously_initialized:
            print('log: firm was previously initialized')
            raise LookupError

        pass


    def __repr__(self) -> str:
        val = self.get_firm_info(info='name') 
        return val if val != None else 'None'

    @classmethod
    def get_initialized_ciks(cls):
        return list(cls._Firm__initialized_ciks)
    
    @classmethod
    def add_to_initialized_ciks(cls, cik):
        cls._Firm__initialized_ciks.add(cik)
        return True
    
    @classmethod
    def get_sec_registry(cls):
        return cls._Firm__sec_registry
    
    @classmethod
    def get_item_from_sec_registry(cls, param, value):
        sec_registry = cls._Firm__sec_registry
        index = [idx for idx,key in enumerate(sec_registry['fields']) if key==param][0]
        firm_info = [item for item in sec_registry['data'] if item[index]==value]
        return firm_info[0]
    
    @classmethod
    def set_sec_registry(cls, registry):
        cls._Firm__sec_registry = registry
        return True


    def check_firm_for_previous_initialize_update_if_not(self, cik:str) -> bool:
        """Check if the firm was previously initialized.  If not
        then add the firm to the collection.
        """
        if cik in Firm.get_initialized_ciks():
            return True
        else:
            Firm.add_to_initialized_ciks(cik)
            return False
            
    def get_firm_info_from_sec_registry(self, param:str, value:str) -> dict[str,str]:
        """Get the firm information from the SEC registry ('company_tickers_exchange.json').
        This will download the registry, if not performed previously, and allow look-up 
        based on the param and value.  If using 'name', then a fuzzy match is performed.

        TODO: get additional info from `<browser> https://www.sec.gov/edgar/browse/?CIK=1084869`
        address, industry, etc. can be used for querying on firms
        TODO: add this as class-level data for use across all objects (ensure pulled only once)
        """
        keys = ['cik','name','ticker','exchange']
        firm_info = None
        if not Firm.get_sec_registry():
            url = 'https://www.sec.gov/files/company_tickers_exchange.json'
            resp = requests.get(url)
            time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
            Firm.set_sec_registry(registry=resp.json())

        if param in ['cik','ticker']:
            info_items = Firm.get_item_from_sec_registry(param, value)
        elif param == 'name':
            registry_data = Firm.get_sec_registry()['data']
            firm_name_choices = [item[1] for item in registry_data]
            possible_names = process.extract(value, firm_name_choices, scorer=fuzz.WRatio, limit=5)
            print(f'Choosing the highest score of these closely-related names: {possible_names}')
            items_list = [item for item in registry_data if item[1]==possible_names[0][0]]
            info_items = items_list[0]

        firm_info = dict(zip(keys, info_items))
        return firm_info

    def get_firm_info(self) -> dict[str,str]:
        """Return all firm information from attributes.

        param: info(str) - <'all','cik','name','ticker','exchange'>

        return: results
        """
        if not self._cik:
            return None
        firm_info = {'cik':self._cik, 'name': self._name, 'ticker': self._ticker, 'exchange': self._exchange}
        return firm_info
    
    def get_initialized_firms(self):
        """Return all firms previously initialized."""
        return Firm.get_initialized_ciks()

    def get_reports_count(self):
        #TODO: check_reports_in_cache
        cnt = len(self._reports)
        return cnt

    def get_insider_transactions(self):
        #TODO: https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK=0001084869
        pass