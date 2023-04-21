"""
Tests the initialization of the Downloader object with
relative and absolute download folder paths.
"""

import os
from pathlib import Path
from datetime import date

import pytest

from sec_edgar_downloader import Downloader
from sec_edgar_downloader._constants import SUPPORTED_FILINGS, DATE_FORMAT_TOKENS
from sec_edgar_downloader._utils import _check_params

def cleanup_directory(dir:str = None) -> bool:
    def rm_tree(pth):
        for child in pth.glob('*'):
            if child.is_file()==True:
                child.unlink()
            else: 
                rm_tree(child)
        pth.rmdir()

    if dir == Path.cwd():
        remove_dir: Path = Path(dir) / 'sec-edgar-filings'
    else:
        remove_dir: Path = Path(dir)
    rm_tree(remove_dir)
    return True



def test_constructor_no_params():
    dl = Downloader()
    expected = Path.cwd()
    cleanup_directory(expected)
    assert dl.download_folder == expected


def test_constructor_blank_path():
    dl = Downloader("")
    # pathlib treats blank paths as the current working directory
    expected = Path.cwd()
    cleanup_directory(expected)
    assert dl.download_folder == expected


@pytest.mark.skipif(
    os.name == "nt", reason="test should only run on Unix-based systems"
)
def test_constructor_relative_path():
    dl = Downloader("./Downloads")
    expected = Path.cwd() / "Downloads"
    cleanup_directory(expected)
    assert dl.download_folder == expected


def test_constructor_user_path():
    dl = Downloader("~/Downloads")
    expected = Path.home() / "Downloads"
    cleanup_directory(expected)
    assert dl.download_folder == expected


def test_constructor_custom_path():
    pth = Path("Downloads/SEC/EDGAR/Downloader")
    custom_path = Path.home() / pth
    dl = Downloader(custom_path)
    cleanup_directory(Path.home() / pth.parts[0])
    assert dl.download_folder == custom_path


def test_supported_filings(downloader):
    dl, _ = downloader
    expected = sorted(SUPPORTED_FILINGS)
    assert dl.supported_filings == expected


def test_get_sec_latest_filings():
    FORM_TYPE = '10-K'
    dl = Downloader()
    Url_list = dl.get_sec_latest_filings_detail_page(file_type=FORM_TYPE)
    cleanup_directory(dl.download_folder)
    assert len(Url_list) > 0


def test__check_params():
    # SEC Edgar Search fails to retrieve Apple 8-Ks after 2000 and before 2002
    formatted_earliest_after_date = date(2002, 1, 1).strftime(DATE_FORMAT_TOKENS)

    filing_type = "8-K"
    ticker = "AAPL"
    before_date = date(2019, 11, 15).strftime(DATE_FORMAT_TOKENS)
    include_amends = False
    # num_filings_to_download < number of filings available
    num_filings_to_download = 100

    dl = Downloader("./Downloads")
    filing, ticker_or_cik, amount, after, before, included_amends, query = _check_params(
        filing_type,
        ticker,
        num_filings_to_download,
        formatted_earliest_after_date,
        before_date,
        include_amends,
    )
    cleanup_directory(dl.download_folder)
    assert (filing == filing_type) and (
            ticker_or_cik == ticker) and (
            amount == num_filings_to_download) and (
            after == formatted_earliest_after_date) and (
            before == before_date) and (
            included_amends == include_amends) and (
            query == "" )


def test_get_urls():
    #TODO: setup and remove download folder
    # SEC Edgar Search fails to retrieve Apple 8-Ks after 2000 and before 2002
    formatted_earliest_after_date = date(2002, 1, 1).strftime(DATE_FORMAT_TOKENS)

    filing_type = "8-K"
    ticker = "AAPL"
    before_date = date(2019, 11, 15).strftime(DATE_FORMAT_TOKENS)
    include_amends = False
    # num_filings_to_download < number of filings available
    num_filings_to_download = 1

    dl = Downloader("./Downloads")
    urls = dl.get_metadata(
        filing=filing_type,
        ticker_or_cik = ticker,
        amount = num_filings_to_download,
        after = formatted_earliest_after_date,
        before = before_date,
        include_amends=include_amends,
    )
    url_count = len(urls['new']) + len(urls['previous']) - len(urls['fail'])
    cleanup_directory(dl.download_folder)
    assert url_count >= num_filings_to_download