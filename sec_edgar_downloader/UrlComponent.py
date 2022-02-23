import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from typing import ClassVar, Dict, List, Optional, Tuple, Union

#from ._utils import generate_random_user_agent
from ._constants import (
    FilingMetadata,
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
    
    def __str__(self) -> str:
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
    """
    __url_sec_current_search: Dict[str,str] = {'10-K': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=0&q3=',
                                '10-Q': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=1&q3=',
                                '8-K': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=4&q3=',
                                'all': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=6&q3='
                                }
    __url_sec_filing_search: str = "https://www.sec.gov/edgar/search/#/q={}"
    __url_company_search: str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&type={}"
    __url_filing_detail_page: str = 'https://www.sec.gov/Archives/edgar/data/{}/{}/{}-index.htm'
    __url_filing_document: str = 'https://www.sec.gov/Archives/edgar/data/{}/{}/{}'

    _url_doc_types = {'ixbrl':None, 
                        'ex_htm':[None], 
                        'xbrl':None, 
                        'zip':None, 
                        'text':None,
                        'xlsx':None
                        }

    def __init__(self, short_cik:str, file_type:str, year:str) -> None:
        self.short_cik = short_cik
        self.file_type = file_type
        self.year = year
        self.accession_number = None

        try:
            self.accession_number = self.get_accession_number()
        except:
            print('unable to get accession_number')
        try:
            self._get_filing_document_all_urls()
        except:
            print('unable to get all filing docs urls')

    @classmethod
    def from_accession_number(cls, accession_number: AccessionNumber) -> None:
        short_cik, file_type, year = cls._get_details(accession_number = accession_number)
        return cls(short_cik, file_type, year)


    def _get_details(accession_number: AccessionNumber) -> Tuple[str, str, str]:
        """accession to cik,file_type,year) to get the actual data, ex:0000072971-20-000230
        __url_sec_filing_search = "https://www.sec.gov/edgar/search/#/q={}&dateRange=all"
        """
        url = 'https://efts.sec.gov/LATEST/search-index'
        payload = {"q":accession_number.accession_number,"dateRange":"all","startdt":"2001-01-01","enddt":"2022-02-22"}
        edgar_resp = requests.post(url, data=json.dumps(payload))
        edgar_resp.raise_for_status()
        edgar_list = edgar_resp.json()['hits']['hits']

        if len(edgar_list) > 1:
            print('Multiple results match description:')
            print(edgar_list)
        else:
            short_cik = edgar_list[0]['_source']['ciks'][0].lstrip('0')     #'51143'
            file_type = edgar_list[0]['_source']['form']                    #'10-Q'
            year =  edgar_list[0]['_source']['file_date'].split('-')[0]     #'2021'
            return short_cik, file_type, year


    def get_sec_latest_filings_detail_page(self, file_type:str) -> str:
        """Get the Filing Detail page urls for the most-recent filings.
        This list is updated every 10min.
        
        :param file_type TODO:only works for 10-K, also need 10-Q, 8-K, etc.
        :return urls

        Usage::

        """
        filled_url = self.__url_sec_current_search[file_type]
        edgar_resp = requests.get(filled_url)
        edgar_resp.raise_for_status()
        edgar_str = edgar_resp.text
        
        soup = BeautifulSoup(edgar_str, 'html.parser')
        href = soup.find('table').find_all('a')
        url_list = [tag[1].attrs['href'] for tag in enumerate(href) if tag[0]%2==0][:-1]
        
        return url_list


    def get_accession_number(self) -> AccessionNumber:
        """Prepare minimum info to access data by taking most-recent filing
            for a date
         #TODO: add size, primary document information for reference

        :param cik
        :param file_type
        :param year     TODO: change to date
        :return AccessionNumber(accno)

        Usage::

        """
        if self.accession_number:
            return self.accession_number

        headers = {
                "User-Agent": generate_random_user_agent(),
                "From": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate"
            }
        
        filled_url = self.__url_company_search.format(self.short_cik, self.file_type)
        print(f'query url: {filled_url}')
        
        edgar_resp = requests.get(filled_url, headers=headers)
        edgar_resp.raise_for_status()
        edgar_str = edgar_resp.text

        soup = BeautifulSoup(edgar_str, 'html.parser')
        tbl = soup.find_all('table')[2]
        df = pd.read_html(tbl.prettify())[0]
        
        df['Filing Date'] = pd.to_datetime(df['Filing Date'])
        row = df.loc[  (df['Filing Date'] > str(self.year)+'-01-01') & (df['Filing Date'] <= str(self.year)+'-12-31')  ]
        #TODO: ^^^sort by closest to date then take the first row

        item = row['Description']
        accno = str(item.values).split('Acc-no: ')[1].split('\\')[0]
        return AccessionNumber(accno)


    def _get_filing_document_all_urls(self) -> None:
        """Get the document's download url
        
        :param acc_no
        :param short_cik
        :return url
        """
        doc = {'ixbrl': ['10-Q','10-K','8-K'],
               'xbrl': ['XML'],
               'text': np.nan,
               'exhibit': ['EX-99.1']   #TODO:add additional versions 99.2, 99.3, ...
              }
        
        headers = {
                "User-Agent": generate_random_user_agent(),
                "From": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate"
            }
        #parts = AccessionNumber(acc_no).get_parts()
        acc_no_noformat = self.accession_number.get_nodash_accession_number()
        #'https://www.sec.gov/Archives/edgar/data/{short_cik}/{acc_no_noformat}/{acc_no}-index.htm'
        filled_url = self.__url_filing_detail_page.format(self.short_cik, acc_no_noformat, str(self.accession_number))
        edgar_resp = requests.get(filled_url, headers=headers)
        edgar_str = edgar_resp.text
        soup = BeautifulSoup(edgar_str, 'html.parser')
        tbls = soup.find_all('table')
        
        df = pd.DataFrame()
        for tbl in tbls:
            tmp = pd.read_html(tbl.prettify())[0]
            df = pd.concat([df, tmp], ignore_index=True)
        newdf = df
        df = newdf.dropna(subset=['Description'])
        
        #TODO: clean this up
        DOC = 'ixbrl'
        types = doc[DOC]
        document_name = df[df['Type'].isin(types)]['Document'].values[0].split('  iXBRL')[0]
        self._url_doc_types[DOC] = self.__url_filing_document.format(self.short_cik, acc_no_noformat, document_name)

        DOC = 'xbrl'
        types = 'INSTANCE'
        document_name = df[df['Description'].str.contains(types)]['Document'].values[0]
        self._url_doc_types[DOC] = self.__url_filing_document.format(self.short_cik, acc_no_noformat, document_name)

        DOC = 'zip' 
        #https://www.sec.gov/Archives/edgar/data/72971/000007297120000230/0000072971-20-000230-xbrl.zip
        #https://www.sec.gov/Archives/edgar/data/320193/000162828016020309/0001628280-16-020309-xbrl.zip
        document_name = str(self.accession_number)+'-xbrl.zip'
        self._url_doc_types[DOC] = self.__url_filing_document.format(self.short_cik, acc_no_noformat, document_name)

        DOC = 'text'
        types = doc[DOC]
        document_name = df[df['Type'].isna()]['Document'].values[0]
        self._url_doc_types[DOC] = self.__url_filing_document.format(self.short_cik, acc_no_noformat, document_name)

        DOC = 'exhibit'
        types = doc[DOC]
        document_name = df[df['Type'].isin(types)]['Document'].values[0]
        self._url_doc_types[DOC] = self.__url_filing_document.format(self.short_cik, acc_no_noformat, document_name)

        DOC = 'xlsx'
        document_name = 'Financial_Report.xlsx'
        self._url_doc_types[DOC] = self.__url_filing_document.format(self.short_cik, acc_no_noformat, document_name)
        
        #submission_base_url = (f"{SEC_EDGAR_ARCHIVES_BASE_URL}/{self.short_cik}/{acc_no_noformat}")

        return FilingMetadata(
            accession_number = self.accession_number,
            filing_details_filename = '',
            full_submission_url = self._url_doc_types['text'],
            filing_details_url = self._url_doc_types['ixbrl'],

            filing_detail_page_url = filled_url,
            xlsx_financial_report_url = self._url_doc_types['xlsx'],
            html_exhibits_url = self._url_doc_types['exhibit'],
            xbrl_instance_doc_url = self._url_doc_types['xbrl'],
            zip_compressed_file_url = self._url_doc_types['zip']
        )

    def get_filing_document_url(self, doc_type: str) -> str:
        """Get the document's download url
        
        :param doc_type 'ixbrl','xbrl','zip','text'
        :return url
        """
        if doc_type in self._url_doc_types.keys():
            url = self._url_doc_types[doc_type]
        else:
            print('there was a problem getting the url')
        return url

    def set_accession_number(self, accession_number: str) -> None:
        self.accession_number = AccessionNumber(accession_number)
        pass