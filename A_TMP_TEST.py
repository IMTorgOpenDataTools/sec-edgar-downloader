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
def test_filing_get_filing_document_url():
    File = uc.Filing(short_cik='51143', file_type='10-Q', year='2021')
    doc_type = 'xbrl'
    File.set_accession_number('0001558370-21-014734')
    File._get_filing_document_all_urls()
    assert File.filing_metadata.xbrl_instance_doc_url == 'https://www.sec.gov/Archives/edgar/data/51143/000155837021014734/ibm-20210930x10q_htm.xml'

test_filing_get_filing_document_url()


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

def test_specific_number():
    cik = '320193'
    number = '0001181431-12-038301'
    results = uc.Filing(cik, uc.AccessionNumber(number) )
    return results


def test_filing_get_filing_document_url():
    short_cik='51143'
    number = '0001628280-16-020309'
    doc_type = {'xbrl':'xbrl_instance_doc_url'}
    File = uc.Filing(short_cik, uc.AccessionNumber(number) )
    assert getattr(File.filing_metadata, doc_type['xbrl']) == 'https://www.sec.gov/Archives/edgar/data/320193/000162828016020309/aapl-20160924.xml'


def test_get_urls():
    # SEC Edgar Search fails to retrieve Apple 8-Ks after 2000 and before 2002
    formatted_earliest_after_date = date(2002, 1, 1).strftime(DATE_FORMAT_TOKENS)

    filing_type = "8-K"
    ticker = "WFC"
    before_date = date(2019, 11, 15).strftime(DATE_FORMAT_TOKENS)
    include_amends = False
    # num_filings_to_download < number of filings available
    num_filings_to_download = 30

    dl = Downloader("./Downloads")
    urls_count = dl.get_urls(
        filing=filing_type,
        ticker_or_cik = ticker,
        amount = num_filings_to_download,
        after = formatted_earliest_after_date,
        before = before_date,
        include_amends=include_amends,
    )

    urls_count = dl.get(
        filing=filing_type,
        ticker_or_cik = ticker,
        amount = num_filings_to_download,
        after = formatted_earliest_after_date,
        before = before_date,
        include_amends=include_amends,
    )


    assert urls_count > 0



#test_filing_get_filing_document_url()
#test_specific_number()
test_get_urls()