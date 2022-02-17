import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

from ._utils import generate_random_user_agent




class AccessionNumber():
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






class Filing():
    """The Filing functionality gets urls for the different files that may be available.
    
    """
    def __init__(self):
        pass

    def get_filing_detail(self, cik, file_type, year):
        """Prepare minimum info to access data

        :param cik
        :param file_type
        :param year
        :return AccessionNumber(accno).get_parts()

        Usage::

        """
        headers = {
                "User-Agent": generate_random_user_agent(),
                "From": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate"
            }
        
        base_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&type={}"
        filled_url = base_url.format(cik, file_type)
        print(f'query url: {filled_url}')
        
        edgar_resp = requests.get(filled_url, headers=headers)
        edgar_str = edgar_resp.text
        soup = BeautifulSoup(edgar_str, 'html.parser')
        tbl = soup.find_all('table')[2]
        df = pd.read_html(tbl.prettify())[0]
        
        df['Filing Date'] = pd.to_datetime(df['Filing Date'])
        #TODO: add qtr current
        row = df.loc[  (df['Filing Date'] > str(year)+'-01-01') & (df['Filing Date'] <= str(year)+'-12-31')  ]
        #TODO: add size, primary document
        item = row['Description']
        accno = str(item.values).split('Acc-no: ')[1].split('\\')[0]
        return AccessionNumber(accno).get_parts()
        """TODO: where does this data fit in?
        <api> https://data.sec.gov/submissions/CIK0000051143.json
        <browser> https://www.sec.gov/edgar/browse/?CIK=1084869
        edgar_resp = requests.get(filled_url, headers=headers)
        edgar_str = edgar_resp.text
        recent = edgar_resp.json()['filings']['recent']
        """
    
    
    def get_sec_latest_filing_detail_page(self, form_type):
        """Get the Filing Detail page urls for the most-recent filings
        
        :param form_type
        :return urls

        Usage::

        """
        urls = {'10-K': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=0&q3=',
                '10-Q': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=1&q3=',
                '8-K': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=4&q3=',
                'all': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=6&q3='
               }
        
        base_url = urls[form_type]
        edgar_resp = requests.get(base_url)
        edgar_str = edgar_resp.text
        
        soup = BeautifulSoup(edgar_str, 'html.parser')
        href = soup.find('table').find_all('a')
        urls = [tag[1].attrs['href'] for tag in enumerate(href) if tag[0]%2==0][:-1]
        
        return urls
    
    
    
    def get_filing_document_url(self, doc_type, acc_no, short_cik):
        """Get the document's download url
        
        :param doc_type
        :param acc_no
        :param short_cik
        :return url
        """
    
        download_dir = f'./large_dataset/sec/{short_cik}/'
        download_path = download_dir+doc_type+'.txt'
    
        doc = {'ixbrl': ['10-Q','10-K','8-K'],
               'xbrl': ['XML'],
               'text': np.nan
              }
        
        headers = {
                "User-Agent": generate_random_user_agent(),
                "From": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate"
            }
    
        parts = AccessionNumber(acc_no).get_parts()
        acc_no_noformat = AccessionNumber(acc_no).get_nodash_accession_number()
        
        url = f'https://www.sec.gov/Archives/edgar/data/{short_cik}/{acc_no_noformat}/{acc_no}-index.htm'
        edgar_resp = requests.get(url, headers=headers)
        edgar_str = edgar_resp.text
        soup = BeautifulSoup(edgar_str, 'html.parser')
        tbls = soup.find_all('table')
        
        df = pd.DataFrame()
        for tbl in tbls:
            tmp = pd.read_html(tbl.prettify())[0]
            df = df.append(tmp)
        
        if doc_type in ['ixbrl']:
            types = doc[doc_type]
            document_name = df[df['Type'].isin(types)]['Document'].values[0].split('  iXBRL')[0]
        elif doc_type in ['xbrl','zip']:
            types = doc[doc_type]
            document_name = df[df['Type'].isin(types)]['Document'].values[0]
        if doc_type in ['text']:
            document_name = df[df['Type'].isna()]['Document'].values[0]
        
        url = f'https://www.sec.gov/Archives/edgar/data/{short_cik}/{acc_no_noformat}/{document_name}'
        print(f'Document url: {url}')
        return url
        
        '''
        TODO: uncomment when ready
        edgar_resp = requests.get(url, headers=headers)
        with open(download_path, 'w') as f: 
            edgar_str = edgar_resp.text
            f.write(edgar_str)
            
        #streaming is preferable, but resp: `403 Client Error: Forbidden for url`
        with requests.get(url, headers, stream=True) as r:
            print(r.status_code)
            r.raise_for_status()
            with open(download_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    # If you have chunk encoded response uncomment `if`
                    # and set chunk_size parameter to None.
                    #if chunk:
                    f.write(chunk)
        '''