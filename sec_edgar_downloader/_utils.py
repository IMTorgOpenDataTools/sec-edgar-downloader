"""Utility functions for the downloader class."""
import sys
import time
import re

from datetime import datetime
from pathlib import Path
from typing import List, Optional
from types import SimpleNamespace
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ._constants import (
    FilingMetadata,
    #get_number_of_unique_filings,
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
    DEFAULT_AFTER_DATE,
    DEFAULT_BEFORE_DATE,
    SUPPORTED_FILINGS
)




class EdgarSearchApiError(Exception):
    """Error raised when Edgar Search API encounters a problem."""


# Specify max number of request retries
# https://stackoverflow.com/a/35504626/3820660
retries = Retry(
    total=MAX_RETRIES,
    backoff_factor=SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL,
    status_forcelist=[403, 500, 502, 503, 504],
)



def validate_date_format(date_format: str) -> None:
    error_msg_base = "Please enter a date string of the form YYYY-MM-DD."

    if not isinstance(date_format, str):
        raise TypeError(error_msg_base)

    try:
        datetime.strptime(date_format, DATE_FORMAT_TOKENS)
    except ValueError as exc:
        # Re-raise with custom error message
        raise ValueError(f"Incorrect date format. {error_msg_base}") from exc


def form_request_payload(
    ticker_or_cik: str,
    filing_types: List[str],
    start_date: str,
    end_date: str,
    start_index: int,
    query: str,
) -> dict:
    payload = {
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
        "entityName": ticker_or_cik,
        "forms": filing_types,
        "from": start_index,
        "q": query,
    }
    return payload


def build_filing_metadata_from_hit(hit: dict) -> FilingMetadata:
    accession_number, filing_details_filename = hit["_id"].split(":", 1)
    CIK, YY, SEQ = accession_number.split('-')

    # Company CIK should be last in the CIK list. This list may also include
    # the CIKs of executives carrying out insider transactions like in form 4.
    cik = hit["_source"]["ciks"][-1]
    accession_number_no_dashes = accession_number.replace("-", "", 2)
    accession_number_short = accession_number.split('-')[0]

    submission_base_url = (
        f"{SEC_EDGAR_ARCHIVES_BASE_URL}/{cik}/{accession_number_no_dashes}"
    )
    cik_short = cik.lstrip('0')
    submission_base_short_url = (
        f"{SEC_EDGAR_ARCHIVES_BASE_URL}/{cik_short}/{accession_number_no_dashes}"
    )

    full_submission_url = f"{submission_base_url}/{accession_number}.txt"
    
    filing_details_url = f"{submission_base_url}/{filing_details_filename}"
    filing_details_filename_extension = Path(filing_details_filename).suffix.replace("htm", "html")
    filing_details_filename = (f"{FILING_DETAILS_FILENAME_STEM}{filing_details_filename_extension}")

    filing_detail_page_url = f"{submission_base_short_url}/{accession_number}-index.htm"
    xlsx_financial_report_url = f"{submission_base_url}/Financial_Report.xlsx"
    html_exhibits_url = ""
    xbrl_instance_doc_url = "" 
    zip_compressed_file_url = f"{submission_base_short_url}/{accession_number}-xbrl.zip"

    return FilingMetadata(
        cik=cik_short,
        ticker='',
        accession_number=accession_number,
        document_metadata_list='',
        filing_details_filename=filing_details_filename,
        full_submission_url=full_submission_url,
        filing_details_url=filing_details_url,

        filing_detail_page_url=filing_detail_page_url,
        xlsx_financial_report_url=xlsx_financial_report_url,
        html_exhibits_url=html_exhibits_url,
        xbrl_instance_doc_url=xbrl_instance_doc_url,
        zip_compressed_file_url=zip_compressed_file_url
    )


def request_standard_url(payload, headers):
    client = requests.Session()
    client.mount("http://", HTTPAdapter(max_retries=retries))
    client.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        resp = client.post(SEC_EDGAR_SEARCH_API_ENDPOINT, json=payload, headers=headers)
        resp.raise_for_status()
        search_query_results = resp.json()

        if "error" in search_query_results:
            try:
                root_cause = search_query_results["error"]["root_cause"]
                if not root_cause:  # pragma: no cover
                    raise ValueError

                error_reason = root_cause[0]["reason"]
                raise EdgarSearchApiError(
                    f"Edgar Search API encountered an error: {error_reason}. "
                    f"Request payload:\n{payload}"
                )
            except (ValueError, KeyError):  # pragma: no cover
                raise EdgarSearchApiError(
                    "Edgar Search API encountered an unknown error. "
                    f"Request payload:\n{payload}"
                ) from None
    finally:
        client.close()
    return search_query_results


def request_scrape_detail_page():
    pass


 
def get_filing_urls_to_download(
    filing_type: str,
    ticker_or_cik: str,
    num_filings_to_download: int,
    after_date: str,
    before_date: str,
    include_amends: bool,
    query: str = "",
) -> List[FilingMetadata]:
    filings_to_fetch: List[FilingMetadata] = []
    start_index = 0
    """CHECKED """

    try:
        while len(filings_to_fetch) < num_filings_to_download:
            payload = form_request_payload(
                ticker_or_cik,
                [filing_type],
                after_date,
                before_date,
                start_index,
                query,
            )
            headers = {
                "User-Agent": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate",
                "Host": "efts.sec.gov",
            }
            
            search_query_results = request_standard_url(payload, headers)
            query_hits = search_query_results["hits"]["hits"]

            # No more results to process
            if not query_hits:
                break

            for hit in query_hits:
                hit_filing_type = hit["_source"]["file_type"]

                is_amend = hit_filing_type[-2:] == "/A"
                if not include_amends and is_amend:
                    continue

                # Work around bug where incorrect filings are sometimes included.
                # For example, AAPL 8-K searches include N-Q entries.
                if not is_amend and hit_filing_type != filing_type:
                    continue

                metadata = build_filing_metadata_from_hit(hit)
                filings_to_fetch.append(metadata)

                if len(filings_to_fetch) == num_filings_to_download:
                    return filings_to_fetch

            # Edgar queries 100 entries at a time, but it is best to set this
            # from the response payload in case it changes in the future
            query_size = search_query_results["query"]["size"]
            start_index += query_size

            # Prevent rate limiting
            time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
    finally:
        print('log: urls extracted successfully')

    return filings_to_fetch


def resolve_relative_urls_in_filing(filing_text: str, download_url: str) -> str:
    soup = BeautifulSoup(filing_text, "lxml")
    base_url = f"{download_url.rsplit('/', 1)[0]}/"

    for url in soup.find_all("a", href=True):
        # Do not resolve a URL if it is a fragment or it already contains a full URL
        if url["href"].startswith("#") or url["href"].startswith("http"):
            continue
        url["href"] = urljoin(base_url, url["href"])

    for image in soup.find_all("img", src=True):
        image["src"] = urljoin(base_url, image["src"])

    if soup.original_encoding is None:  # pragma: no cover
        return soup

    return soup.encode(soup.original_encoding)


def _check_params(
        filing: str,
        ticker_or_cik: str,
        amount: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        include_amends: bool = False,
        query: str = "",
    ) -> int:
        """Check parameters similar across functions.

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
        :return: params.
        """
        ticker_or_cik = str(ticker_or_cik).strip().upper()

        # Check for blank tickers or CIKs
        if not ticker_or_cik:
            raise ValueError("Invalid ticker or CIK. Please enter a non-blank value.")

        # Detect CIKs and ensure that they are properly zero-padded
        if is_cik(ticker_or_cik):
            if len(ticker_or_cik) > 10:
                raise ValueError("Invalid CIK. CIKs must be at most 10 digits long.")
            # Pad CIK with 0s to ensure that it is exactly 10 digits long
            # The SEC Edgar Search API requires zero-padded CIKs to ensure
            # that search results are accurate. Relates to issue #84.
            ticker_or_cik = ticker_or_cik.zfill(10)

        if amount is None:
            # If amount is not specified, obtain all available filings.
            # We simply need a large number to denote this and the loop
            # responsible for fetching the URLs will break appropriately.
            amount = sys.maxsize
        else:
            amount = int(amount)
            if amount < 1:
                raise ValueError(
                    "Invalid amount. Please enter a number greater than 1."
                )

        # SEC allows for filing searches from 2000 onwards
        if after is None:
            after = DEFAULT_AFTER_DATE.strftime(DATE_FORMAT_TOKENS)
        else:
            validate_date_format(after)

            if after < DEFAULT_AFTER_DATE.strftime(DATE_FORMAT_TOKENS):
                raise ValueError(
                    f"Filings cannot be downloaded prior to {DEFAULT_AFTER_DATE.year}. "
                    f"Please enter a date on or after {DEFAULT_AFTER_DATE}."
                )

        if before is None:
            before = DEFAULT_BEFORE_DATE.strftime(DATE_FORMAT_TOKENS)
        else:
            validate_date_format(before)

        if after > before:
            raise ValueError(
                "Invalid after and before date combination. "
                "Please enter an after date that is less than the before date."
            )

        if filing not in SUPPORTED_FILINGS:
            filing_options = ", ".join(SUPPORTED_FILINGS)
            raise ValueError(
                f"'{filing}' filings are not supported. "
                f"Please choose from the following: {filing_options}."
            )

        if not isinstance(query, str):
            raise TypeError("Query must be of type string.")
            
        return filing, ticker_or_cik, amount, after, before, include_amends, query




def download_urls(download_folder, filing_storage, list_of_doc_tuples):
    """Download document given only a list of document urls.  Update the File records.

    :param list_of_doc_tuples: these should be in the form of (key, DocumentMetadata), where key is 'cik|acc_no|doc_seq'
    
    """
    base_url = 'https://www.sec.gov'

    client = requests.Session()
    client.mount("http://", HTTPAdapter(max_retries=retries))
    client.mount("https://", HTTPAdapter(max_retries=retries))
    headers = {
        "User-Agent": generate_random_user_agent(),
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
        }

    new_doc_list = []
    previous_doc_list = []
    fail_doc_list = []
    for key, doc in list_of_doc_tuples:
        cik, acc_no, doc_seq = key.split('|')
        file_key = cik + '|' + acc_no
        if doc.FS_Location == None:
            try:
                url = base_url + doc.URL
                resp = client.get(url, headers=headers)
                resp.raise_for_status()
                filing_text = resp.content

                # Create all parent directories as needed and write content to file
                save_path = (
                    download_folder
                    / ROOT_SAVE_FOLDER_NAME
                    / cik
                    #/ doc.Type
                    / acc_no
                    / doc.Document
                    )
                save_path.parent.mkdir(parents=True, exist_ok=True)
                save_path.write_bytes(filing_text) 

                new_doc = doc._replace(FS_Location = save_path)
                filing_storage.modify_document_in_record(file_key, doc, new_doc)
                new_doc_list.append(new_doc)            
                # Prevent rate limiting
                print(f'loaded document: {key}')
                time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)             
            except requests.exceptions.HTTPError as e:  # pragma: no cover
                fail_doc_list.append(doc)
                print("Skipping full submission download for "
                        f"'{url}' due to network error: {e}."
                        )
        else:
            previous_doc_list.append(doc)
    filing_storage.dump_to_pickle()
    result = {'new': new_doc_list,
                'previous': previous_doc_list,
                'fail': fail_doc_list}
    return result
        








def download_and_save_filing(
    client: requests.Session,
    download_folder: Path,
    ticker_or_cik: str,
    accession_number: str,
    filing_type: str,
    download_url: str,
    save_filename: str,
    *,
    resolve_urls: bool = False,
) -> None:
    headers = {
        "User-Agent": generate_random_user_agent(),
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    }
    resp = client.get(download_url, headers=headers)
    resp.raise_for_status()
    filing_text = resp.content

    # Only resolve URLs in HTML files
    if resolve_urls and Path(save_filename).suffix == ".html":
        filing_text = resolve_relative_urls_in_filing(filing_text, download_url)

    # Create all parent directories as needed and write content to file
    save_path = (
        download_folder
        / ROOT_SAVE_FOLDER_NAME
        / ticker_or_cik
        / filing_type
        / accession_number
        / save_filename
    )
    save_path.parent.mkdir(parents=True, exist_ok=True)
    save_path.write_bytes(filing_text)

    # Prevent rate limiting
    time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)


def download_filings(
    download_folder: Path,
    ticker_or_cik: str,
    filing_type: str,
    filings_to_fetch: List,
    include_filing_details: bool,
    ) -> None:

    client = requests.Session()
    client.mount("http://", HTTPAdapter(max_retries=retries))
    client.mount("https://", HTTPAdapter(max_retries=retries))
    try:
        for filing in filings_to_fetch:
            try:
                download_and_save_filing(
                    client,
                    download_folder,
                    ticker_or_cik,
                    filing.accession_number.get_accession_number(),
                    filing_type,
                    filing.filing_metadata.full_submission_url,
                    FILING_FULL_SUBMISSION_FILENAME,
                )
            except requests.exceptions.HTTPError as e:  # pragma: no cover
                print("Skipping full submission download for "
                        f"'{filing.accession_number}' due to network error: {e}."
                        )

            if include_filing_details:
                try:
                    download_and_save_filing(
                        client,
                        download_folder,
                        ticker_or_cik,
                        filing.accession_number.get_accession_number(),
                        filing_type,
                        filing.filing_metadata.filing_detail_page_url,
                        filing.filing_metadata.filing_details_filename,
                        resolve_urls=True,
                    )
                except requests.exceptions.HTTPError as e:  # pragma: no cover
                    print(f"Skipping filing detail download for "
                            f"'{filing.accession_number}' due to network error: {e}."
                            )
    finally:
        client.close()



