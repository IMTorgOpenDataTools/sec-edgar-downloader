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



def test_blindly_getting_data():
    formatted_earliest_after_date = date(2002, 1, 1).strftime(DATE_FORMAT_TOKENS)

    filing_type = "8-K"
    ticker = "PNC"
    before_date = date(2021, 12, 31).strftime(DATE_FORMAT_TOKENS)
    include_amends = False
    # num_filings_to_download < number of filings available
    num_filings_to_download = 30

    dl = Downloader("./Downloads")
    urls_count = dl.get(
        filing=filing_type,
        ticker_or_cik = ticker,
        amount = num_filings_to_download,
        after = formatted_earliest_after_date,
        before = before_date,
        include_amends=include_amends,
    )
    assert urls_count > 0



def test_get_urls():
    # SEC Edgar Search fails to retrieve Apple 8-Ks after 2000 and before 2002
    formatted_earliest_after_date = date(2022, 1, 1).strftime(DATE_FORMAT_TOKENS)

    filing_type = "8-K"
    ticker = "PNC"
    #before_date = date(2021, 12, 31).strftime(DATE_FORMAT_TOKENS)
    include_amends = False
    # num_filings_to_download < number of filings available
    num_filings_to_download = 30

    dl = Downloader("./Downloads")
    result_filing_dict = dl.get_metadata(
        filing=filing_type,
        ticker_or_cik = ticker,
        amount = num_filings_to_download,
        after = formatted_earliest_after_date,
        #before = before_date,
        include_amends=include_amends,
    )

    df = dl.filing_storage.get_dataframe(mode='document')
    lst_of_idx = df.index.tolist()
    staged = dl.filing_storage.get_document_in_record( lst_of_idx )
    result_doc_dict = dl.get_documents_from_url_list(staged)

    available_docs = result_doc_dict['new']
    available_docs.extend(result_doc_dict['previous'])

    #TODO: remove dirs in `./Downloads`
    assert len(available_docs) > 358






#test_specific_number()
#test_filing_get_filing_document_url()
test_get_urls()