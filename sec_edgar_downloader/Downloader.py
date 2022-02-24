"""Provides a :class:`Downloader` class for downloading SEC EDGAR filings."""

import sys
from pathlib import Path
from typing import ClassVar, List, Optional, Union

from ._constants import DATE_FORMAT_TOKENS, DEFAULT_AFTER_DATE, DEFAULT_BEFORE_DATE
from ._constants import SUPPORTED_FILINGS as _SUPPORTED_FILINGS
from ._utils import (
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

    supported_filings: ClassVar[List[str]] = sorted(_SUPPORTED_FILINGS)

    def __init__(self, download_folder: Union[str, Path, None] = None) -> None:
        """Constructor for the :class:`Downloader` class."""
        if download_folder is None:
            self.download_folder = Path.cwd()
        elif isinstance(download_folder, Path):
            self.download_folder = download_folder
        else:
            self.download_folder = Path(download_folder).expanduser().resolve()

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

        #TODO:steps
        #add urls that should be gotten to FilingMetadata
        #run this, then check results for missing urls
        #for urls that are missing, creating Filing and populate data
        filings_to_fetch = get_filing_urls_to_download(
            filing,
            ticker_or_cik,
            amount,
            after,
            before,
            include_amends,
            query,
        )
        FilingList = [uc.Filing.from_accession_number( uc.AccessionNumber(filing.accession_number) ) for filing in  filings_to_fetch] 

        # Get number of unique accession numbers downloaded
        return FilingList 


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

        filings_to_fetch = get_filing_urls_to_download(
            filing,
            ticker_or_cik,
            amount,
            after,
            before,
            include_amends,
            query,
        )

        download_filings(
            self.download_folder,
            ticker_or_cik,
            filing,
            filings_to_fetch,
            download_details,
        )

        # Get number of unique accession numbers downloaded
        return get_number_of_unique_filings(filings_to_fetch)
