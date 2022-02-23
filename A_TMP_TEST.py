import os
from pathlib import Path
from datetime import date

import pytest

from sec_edgar_downloader import Downloader
from sec_edgar_downloader._constants import SUPPORTED_FILINGS, DATE_FORMAT_TOKENS

from sec_edgar_downloader import UrlComponent as uc

from sec_edgar_downloader import Downloader
from sec_edgar_downloader._constants import DATE_FORMAT_TOKENS
from sec_edgar_downloader._utils import EdgarSearchApiError, get_filing_urls_to_download




"""
@pytest.mark.parametrize(
    "filing_type", ["4", "8-K", "10-K", "10-Q", "SC 13G", "SD", "DEF 14A"]
)
def test_common_filings(
    filing_type, formatted_earliest_after_date, formatted_latest_before_date
):
    # AAPL files 4, 8-K, 10-K, 10-Q, SC 13G, SD, DEF 14A
    ticker = "AAPL"
    num_filings_to_download = 1
    include_amends = False

    filings_to_download = get_filing_urls_to_download(
        filing_type,
        ticker,
        num_filings_to_download,
        formatted_earliest_after_date,
        formatted_latest_before_date,
        include_amends,
    )
    assert len(filings_to_download) == 1

formatted_latest_before_date = date(2019, 11, 15).strftime(DATE_FORMAT_TOKENS)
formatted_earliest_after_date = date(2002, 1, 1).strftime(DATE_FORMAT_TOKENS)
for filing_type in ["10-K","8-K"]:
    test_common_filings(filing_type, formatted_earliest_after_date, formatted_latest_before_date)
"""
def test_get_urls():
    # SEC Edgar Search fails to retrieve Apple 8-Ks after 2000 and before 2002
    formatted_earliest_after_date = date(2002, 1, 1).strftime(DATE_FORMAT_TOKENS)

    filing_type = "8-K"
    ticker = "AAPL"
    before_date = date(2019, 11, 15).strftime(DATE_FORMAT_TOKENS)
    include_amends = False
    # num_filings_to_download < number of filings available
    num_filings_to_download = 100

    dl = Downloader("./Downloads")
    urls = dl.get_urls(
        filing=filing_type,
        ticker_or_cik = ticker,
        amount = num_filings_to_download,
        after = formatted_earliest_after_date,
        before = before_date,
        include_amends=include_amends,
    )
    assert len(urls) == num_filings_to_download


test_get_urls()
