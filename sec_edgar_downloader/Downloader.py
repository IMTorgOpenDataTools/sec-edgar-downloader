"""Provides a :class:`Downloader` class for downloading SEC EDGAR filings."""
import requests
import time
from bs4 import BeautifulSoup

import sys
from pathlib import Path
from typing import ClassVar, List, Optional, Union, Dict

from ._constants import DATE_FORMAT_TOKENS, DEFAULT_AFTER_DATE, DEFAULT_BEFORE_DATE, SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL, ROOT_SAVE_FOLDER_NAME
from ._constants import SUPPORTED_FILINGS as _SUPPORTED_FILINGS
from ._utils import (
    download_urls,
    download_filings,
    _check_params,
    get_filing_urls_to_download,
    get_number_of_unique_filings,
    is_cik,
    validate_date_format,
)
from . import UrlComponent as uc






class Downloader:
    """A :class:`Downloader` object.

    :param download_folder: relative or absolute path to download location.
        Defaults to the current working directory.

    Usage::

        >>> from sec_edgar_downloader import Downloader

        # Download to current working directory
        >>> dl = Downloader()

        # Download to relative or absolute path
        >>> dl = Downloader("/path/to/valid/save/location")
    """
    _url_sec_current_search: Dict[str,str] = {'10-K': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=0&q3=',
                                '10-Q': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=1&q3=',
                                '8-K': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=4&q3=',
                                'all': 'https://www.sec.gov/cgi-bin/current?q1=0&q2=6&q3='
                                }
    supported_filings: ClassVar[List[str]] = sorted(_SUPPORTED_FILINGS)

    def __init__(self, download_folder: Union[str, Path, None] = None) -> None:
        """Constructor for the :class:`Downloader` class."""
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
        self.filing_storage: uc.FilingStorage = uc.FilingStorage(self.root_folder)


    def __repr__(self) -> str:
        #TODO: display count of urls queried, count filings downloaded by type
        pass

    def get_sec_latest_filings_detail_page(self, file_type:str) -> str:
        """Get the Filing Detail page urls for the most-recent filings.
        This list is updated every 10min.
        
        :param file_type
        :return urls

        Usage::

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


    def get_urls(
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
        """
        filing, ticker_or_cik, amount, after, before, include_amends, query = _check_params(
            filing = filing,
            ticker_or_cik = ticker_or_cik,
            amount = amount,
            after = after,
            before = before,
            include_amends = include_amends,
            query = query,
        )

        filings_to_fetch = get_filing_urls_to_download(
            filing,
            ticker_or_cik,
            amount,
            after,
            before,
            include_amends,
            query,
        )
        NewFilingList = [ uc.Filing(short_cik = filing.cik, 
                                    accession_number = uc.AccessionNumber(filing.accession_number)
                                    ) 
                        for filing in  filings_to_fetch
                        ] 

        # Get number of unique accession numbers downloaded
        self.filing_storage.add_new_list( NewFilingList )
        return len(NewFilingList) 

    def get_by_url(self, list_of_urls):
        pass

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

        if self.filing_storage == None or len(self.filing_storage.get_list()) < 1:
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