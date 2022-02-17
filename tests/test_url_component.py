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
File = uc.Filing()

def test_filing_get_detail():
    assert File.get_filing_detail(cik=320193, file_type='10-K', year='2016') == {'short_cik': '1628280', 'year': '2016', 'annual_sequence': '020309'}

def test_filing_get_sec_latest_filings():
    form_type = '10-K'
    assert len(File.get_sec_latest_filing_detail_page(form_type)) > 0

def test_filing_get_filing_document_url():
    doc_type = 'xbrl'
    acc_no = '0001558370-21-014734'
    short_cik =  '51143'
    assert File.get_filing_document_url(doc_type, acc_no, short_cik) == 'https://www.sec.gov/Archives/edgar/data/51143/000155837021014734/ibm-20210930x10q_htm.xml'