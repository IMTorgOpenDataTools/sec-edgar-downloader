# TODO: add docstrings to functions
# TODO: spawn a thread for each download. I/O will block so just thread downloads to run in parallel
# TODO: handle invalid ticker or CIK

from datetime import date
from bs4 import BeautifulSoup
from pathlib import Path
from collections import namedtuple
import requests
import os
import errno

FilingInfo = namedtuple('FilingInfo', ['filename', 'url'])

class Downloader():
    def __init__(self, download_folder=str(Path.joinpath(Path.home(), "Downloads"))):
        print("Welcome to the SEC EDGAR Downloader!")

        # TODO: should we delete a folder or overrite it when the same data is requested?
        if not Path(download_folder).exists():
            raise IOError(f"The folder for saving company filings ({download_folder}) does not exist.")

        print(f"Company filings will be saved to: {download_folder}")

        self.download_folder = download_folder

        # TODO: Allow users to pass this in
        # Will have to handle pagination since only 100 are displayed on a single page.
        # Requires another start query parameter: start=100&count=100
        self.count = 100
        self.base_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&owner=exclude&count={self.count}"

        self.filing_method_dict = {
            '8-k': self.get_8k_filing_for_ticker,
            '10-k': self.get_10k_filing_for_ticker,
            '10-q': self.get_10q_filing_for_ticker,
            '13f': self.get_13f_filing_for_ticker,
            'sd': self.get_sd_filing_for_ticker,
            'sc 13g': self.get_sc_13g_filing_for_ticker,
        }

    # TODO: add validation and verification for ticker
    def validate_ticker(self, ticker):
        pass

    # TODO: allow users to specify before date (by passing in year, month, and day)
    # and formatting it here
    def form_url(self, ticker, filing_type):
        # Put into required format: YYYYMMDD
        before_date = date.today().strftime('%Y%m%d')
        return f"{self.base_url}&CIK={ticker}&type={filing_type.replace(' ', '+')}&dateb={before_date}"

    def download_filings(self, edgar_search_url, filing_type, ticker):
        resp = requests.get(edgar_search_url)
        resp.raise_for_status()
        edgar_results_html = resp.content

        edgar_results_scraper = BeautifulSoup(edgar_results_html, "lxml")

        document_anchor_elements = edgar_results_scraper.find_all(id="documentsbutton", href=True)

        sec_base_url = "https://www.sec.gov"
        filing_document_info = []
        for anchor_element in document_anchor_elements:
            filing_detail_url = f"{sec_base_url}{anchor_element['href']}"
            # Some entries end with .html, some end with .htm
            if filing_detail_url[-1] != "l":
                filing_detail_url += 'l'
            full_filing_url = filing_detail_url.replace("-index.html", ".txt")
            name = full_filing_url.split("/")[-1]
            filing_document_info.append(FilingInfo(filename=name, url=full_filing_url))

        if len(filing_document_info) == 0:
            print(f"No {filing_type} documents available for {ticker}.")
            return

        print(f"{len(filing_document_info)} {filing_type} documents available for {ticker}. Beginning download...")

        for doc_info in filing_document_info:
            resp = requests.get(doc_info.url, stream=True)
            resp.raise_for_status()

            save_path = Path(self.download_folder).joinpath("sec-edgar-filings", ticker, filing_type, doc_info.filename)

            # Create all parent directories as needed.
            # For example: if we have /hello and we want to create
            # /hello/world/my/name/is/john.txt, this would create
            # all the directores leading up to john.txt
            if not Path.exists(Path(save_path).parent):
                try:
                    os.makedirs(Path(save_path).parent)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise

            with open(save_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=1024):
                    if chunk: # filter out keep-alive chunks
                        f.write(chunk)

        print(f"All {filing_type}s for {ticker} downloaded successfully.")

    def get_filing_wrapper(self, filing_type, ticker):
        print(f"Getting {filing_type} filings for {ticker}.")
        filing_url = self.form_url(ticker, filing_type)
        self.download_filings(filing_url, filing_type, ticker)

    # TODO: distinguish filing AMENDS (e.g. 8-K/A)?
    def get_8k_filing_for_ticker(self, ticker):
        filing_type = "8-K"
        self.get_filing_wrapper(filing_type, ticker)

    def get_10k_filing_for_ticker(self, ticker):
        filing_type = "10-K"
        self.get_filing_wrapper(filing_type, ticker)

    def get_10q_filing_for_ticker(self, ticker):
        filing_type = "10-Q"
        self.get_filing_wrapper(filing_type, ticker)

    def get_13f_filing_for_ticker(self, ticker):
        filing_type = "13F"
        self.get_filing_wrapper(filing_type, ticker)

    def get_sc_13g_filing_for_ticker(self, ticker):
        filing_type = "SC 13G"
        self.get_filing_wrapper(filing_type, ticker)

    def get_sd_filing_for_ticker(self, ticker):
        filing_type = "SD"
        self.get_filing_wrapper(filing_type, ticker)

    def get_all_available_filings_for_ticker(self, ticker):
        self.get_8k_filing_for_ticker(ticker)
        self.get_10k_filing_for_ticker(ticker)
        self.get_10q_filing_for_ticker(ticker)
        self.get_13f_filing_for_ticker(ticker)
        self.get_sc_13g_filing_for_ticker(ticker)
        self.get_sd_filing_for_ticker(ticker)

    def get_all_available_filings_for_ticker_list(self, tickers):
        for ticker in tickers:
            self.get_all_available_filings_for_ticker(ticker)

    def get_select_filings_for_ticker(self, ticker, filings):
        for filing in filings:
            filing_formatted = filing.lower()
            if filing_formatted not in self.filing_method_dict:
                available_filings = ", ".join([key.upper() for key in self.filing_method_dict.keys()])
                print(f"The provided filing ({filing}) is not available. Available filings: {available_filings}.")
                continue
            self.filing_method_dict[filing_formatted](ticker)
