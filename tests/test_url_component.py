from sec_edgar_downloader import UrlComponent as uc



# Accession
number = '0001193125-15-118890'
Acc_no = uc.AccessionNumber(number)

def test_acc_verify_parts():
    assert Acc_no.get_parts_verification() == 'Parts are correct'

def test_acc_return_parts():
    result = Acc_no.get_parts()
    assert result == {'short_cik': '1193125', 'year': '2015', 'annual_sequence': '118890'}

def test_acc_get_number():
    result = Acc_no.get_parts()
    Acc_no.create_accession_number(*result.values())
    assert Acc_no.accession_number == number

def test_acc_get_nodash_number():
    assert Acc_no.get_nodash_accession_number() == number.replace('-','')


# Filing

def test_filing():
    FORM_TYPE = '10-K'
    number = '0001628280-16-020309'
    Acc_no1 = uc.AccessionNumber(number)
    File = uc.Filing(short_cik=320193, file_type=FORM_TYPE, file_date='2016-10-26')
    assert File.accession_number.get_accession_number() == Acc_no1.get_accession_number()


def test_filing_get_sec_latest_filings():
    FORM_TYPE = '10-K'
    File = uc.Filing(short_cik=320193, file_type=FORM_TYPE, file_date='2016-10-26')
    assert len(File.get_sec_latest_filings_detail_page(FORM_TYPE)) > 0


def test_filing_construct_with_accession():
    number = '0001628280-16-020309'
    Acc_no1 = uc.AccessionNumber(number)
    File = uc.Filing.from_accession_number(Acc_no1)
    assert File.short_cik == '320193'


def test_filing_get_filing_document_url():
    File = uc.Filing(short_cik='51143', file_type='10-Q', file_date='2021-11-05')
    doc_type = 'xbrl'
    File.set_accession_number('0001558370-21-014734')
    File._get_filing_document_all_urls()
    assert File.filing_metadata.xbrl_instance_doc_url == 'https://www.sec.gov/Archives/edgar/data/51143/000155837021014734/ibm-20210930x10q_htm.xml'