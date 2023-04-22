#!/usr/bin/env python3
"""
The Downloader class for primary object used in downloading SEC EDGAR filings.

Downloader enables extraction of Filings and Document metadata, maintains
that data in the FilingStorage, and downloads selected data into file system.

Classes:
    Downloader

"""
import requests
import time
from bs4 import BeautifulSoup

from pathlib import Path
from typing import ClassVar, List, Optional, Union, Dict

from ._constants import SEC_EDGAR_CURRENT_SEARCH_BASE_URL, DATE_FORMAT_TOKENS, DEFAULT_AFTER_DATE, DEFAULT_BEFORE_DATE, SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL, ROOT_SAVE_FOLDER_NAME
from ._constants import SUPPORTED_FILINGS as _SUPPORTED_FILINGS
from ._utils import (
    download_urls,
    download_filings,
    _check_params,
    get_filing_urls_to_download,
    #get_number_of_unique_filings,
    is_cik,
    validate_date_format,
)
from . import UrlComponent as uc
from . import FilingStorage as fs






class Downloader:
    """Create Downloader object for Filing Documents download and maintenance.

    Usage::
        >>> from sec_edgar_downloader import Downloader
        # Download to current working directory
        >>> dl = Downloader()
        # Download to relative or absolute path
        >>> dl = Downloader("/path/to/valid/save/location")
        >>> dl.download_folder

    _Note_: directory and file creation is performed on instantiation. All
    modifications to the `dl.download_folder` should be performed using the 
    `dl` functionality so that the FilingStorage remains in-synch.  
    """
    _url_sec_current_search: Dict[str,str] = {'10-K': SEC_EDGAR_CURRENT_SEARCH_BASE_URL+'/current?q1=0&q2=0&q3=',
                                '10-Q': SEC_EDGAR_CURRENT_SEARCH_BASE_URL+'/current?q1=0&q2=1&q3=',
                                '8-K': SEC_EDGAR_CURRENT_SEARCH_BASE_URL+'/current?q1=0&q2=4&q3=',
                                'all': SEC_EDGAR_CURRENT_SEARCH_BASE_URL+'/current?q1=0&q2=6&q3='
                                }
    supported_filings: ClassVar[List[str]] = sorted(_SUPPORTED_FILINGS)

    def __init__(self, download_folder: Union[str, Path, None] = None) -> None:
        """Constructor for the class Downloader class.

        :param download_folder - relative or absolute path to download 
        location. Defaults to the current working directory.  Default 
        creates `./sec-edgar-filings/filing_storage.pickle`.
        
        """
        self.download_folder = None
        self.root_folder = None
        self.filing_storage = None

        if download_folder is None:
            self.download_folder = Path.cwd()
        elif isinstance(download_folder, Path):
            self.download_folder = download_folder
        else:
            self.download_folder = Path(download_folder).expanduser().resolve()

        self.root_folder = Path(self.download_folder) / ROOT_SAVE_FOLDER_NAME  
        self.root_folder.mkdir(parents=True, exist_ok=True)
        self.filing_storage: fs.FilingStorage = fs.FilingStorage(self.root_folder)


    def __repr__(self) -> str:
        return self.filing_storage.__repr__()


    def get_sec_latest_filings_detail_page(self, file_type:str) -> str:
        """Get the Filing Detail page urls for the most-recent filings.
        This list is updated every 10min.
        
        :param file_type
        :return urls

        Usage

        """
        filled_url = self._url_sec_current_search[file_type]
        edgar_resp = requests.get(filled_url)
        edgar_resp.raise_for_status()
        time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
        edgar_str = edgar_resp.text
        
        soup = BeautifulSoup(edgar_str, 'html.parser')
        href = soup.find('table').find_all('a')
        url_list = [tag[1].attrs['href'] for tag in enumerate(href) if tag[0]%2==0][:-1]
        
        return url_list


    def get_metadata(
        self,
        filing: str,
        ticker_or_cik: str,
        *,
        amount: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        include_amends: bool = False,
        query: str = "",
    ) -> int:
        """Download metadata for filings and save to storage.

        :param filing: filing type to download (e.g. 8-K).
        :param ticker_or_cik: ticker or CIK to download filings for.
        :param amount: number of filings to download.
            Defaults to all available filings.
        :param after: date of form YYYY-MM-DD after which to download filings.
            Defaults to 2000-01-01, the earliest date supported by EDGAR full text search.
        :param before: date of form YYYY-MM-DD before which to download filings.
            Defaults to today.
        :param include_amends: denotes whether or not to include filing amends (e.g. 8-K/A).
            Defaults to False.
        :param download_details: denotes whether or not to download human-readable and easily
            parseable filing detail documents (e.g. form 4 XML, 8-K HTML). Defaults to True.
        :param query: keyword to search for in filing documents.
        :return: number of filings downloaded.

        Usage::
        """
        # check the query
        filing, ticker_or_cik, amount, after, before, include_amends, query = _check_params(
            filing = filing,
            ticker_or_cik = ticker_or_cik,
            amount = amount,
            after = after,
            before = before,
            include_amends = include_amends,
            query = query,
        )

        '''
        #TODO:complete this functionality, convert to df and query on params
        check = self.filing_storage.check_record_exists(
            filing = filing,
            ticker_or_cik = ticker_or_cik,
            after = after,
        )'''

        # get filing urls from the query
        filings_to_fetch = get_filing_urls_to_download(
            filing,
            ticker_or_cik,
            amount,
            after,
            before,
            include_amends,
            query,
        )

        # add filings to storage if they don't exist
        new_filing_list = []
        previously_loaded = []
        for filing in filings_to_fetch:
            key = filing.cik + '|' + filing.accession_number
            test_filing = self.filing_storage.get_record(key)
            if not test_filing:
                new_file = uc.Filing(short_cik = filing.cik,
                                    accession_number = uc.AccessionNumber(filing.accession_number)
                                    ) 
                new_filing_list.append(new_file)
            else:
                previously_loaded.append(filing)
        self.filing_storage.add_record(rec_lst = new_filing_list)
        result_filing_dict = {'new': new_filing_list,
                                'previous': previously_loaded,
                                'fail': []
                                }
        return result_filing_dict


    def get_documents_from_url_list(self, list_of_doc_tuples):
        """Simple download files using list of urls.

        :param filing: filing type to download (e.g. 8-K).
        :param list_of_document_urls: simple urls (e.g. )
        :return: status of download (downloaded -previously, -current, failed to download)

        TODO: a failure is that it does not recognize a specific file type which has nan for Seq
        """
        check_list = []
        new = []
        previously_loaded = []
        fail = []
        for doc in list_of_doc_tuples:
            cik, accn, seq = doc[0].split('|')
            key = f'{cik}|{accn}'
            filing = self.filing_storage.get_record(key)
            check = [document for document in filing.document_metadata_list if (str(document.Seq) == seq and document.FS_Location == None)]
            if not check==[]: 
                check_list.append(doc)
            else:
                previously_loaded.append(doc)

        if not check_list==[]:
            result_doc_dict = download_urls(self.download_folder, 
                                        self.filing_storage, 
                                        check_list
                                        )
            self.filing_storage.load_from_pickle()
            new.extend( result_doc_dict['new'] )
            previously_loaded.extend( result_doc_dict['previous'] )
        result = {'new': new,
                    'previous': previously_loaded,
                    'fail': fail
                    }
        return result


    def get(
        self,
        filing: str,
        ticker_or_cik: str,
        *,
        amount: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        include_amends: bool = False,
        download_details: bool = True,
        query: str = "",
    ) -> int:
        """Download filings and save them to disk.

        :param filing: filing type to download (e.g. 8-K).
        :param ticker_or_cik: ticker or CIK to download filings for.
        :param amount: number of filings to download.
            Defaults to all available filings.
        :param after: date of form YYYY-MM-DD after which to download filings.
            Defaults to 2000-01-01, the earliest date supported by EDGAR full text search.
        :param before: date of form YYYY-MM-DD before which to download filings.
            Defaults to today.
        :param include_amends: denotes whether or not to include filing amends (e.g. 8-K/A).
            Defaults to False.
        :param download_details: denotes whether or not to download human-readable and easily
            parseable filing detail documents (e.g. form 4 XML, 8-K HTML). Defaults to True.
        :param query: keyword to search for in filing documents.
        :return: number of filings downloaded.

        Usage::

            >>> from sec_edgar_downloader import Downloader
            >>> dl = Downloader()

            # Get all 8-K filings for Apple
            >>> dl.get("8-K", "AAPL")

            # Get all 8-K filings for Apple, including filing amends (8-K/A)
            >>> dl.get("8-K", "AAPL", include_amends=True)

            # Get all 8-K filings for Apple after January 1, 2017 and before March 25, 2017
            >>> dl.get("8-K", "AAPL", after="2017-01-01", before="2017-03-25")

            # Get the five most recent 10-K filings for Apple
            >>> dl.get("10-K", "AAPL", amount=5)

            # Get all 10-K filings for Apple, excluding the filing detail documents
            >>> dl.get("10-K", "AAPL", amount=1, download_details=False)

            # Get all Apple proxy statements that contain the term "antitrust"
            >>> dl.get("DEF 14A", "AAPL", query="antitrust")

            # Get all 10-Q filings for Visa
            >>> dl.get("10-Q", "V")

            # Get all 13F-NT filings for the Vanguard Group
            >>> dl.get("13F-NT", "0000102909")

            # Get all 13F-HR filings for the Vanguard Group
            >>> dl.get("13F-HR", "0000102909")

            # Get all SC 13G filings for Apple
            >>> dl.get("SC 13G", "AAPL")

            # Get all SD filings for Apple
            >>> dl.get("SD", "AAPL")
        """
        filing, ticker_or_cik, amount, after, before, include_amends, query = _check_params(
            filing,
            ticker_or_cik,
            amount,
            after,
            before,
            include_amends,
            query,
        )

        if self.filing_storage == None:
            if len(self.filing_storage.get_list()) < 1:
                print('log: you must run `get_urls()` before downloading the documents')
                return None
        else:
            filings_to_fetch = self.filing_storage.get_list()

        download_urls(self.download_folder, filing, filings_to_fetch)
        """
        download_filings(
            self.download_folder,
            ticker_or_cik,
            filing,
            filings_to_fetch,
            download_details,
        )
        """

        # Get number of unique accession numbers downloaded
        #return get_number_of_unique_filings(filings_to_fetch)
        return len(filings_to_fetch)