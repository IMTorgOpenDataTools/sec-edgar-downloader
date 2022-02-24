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
    File = uc.Filing.from_file_details(short_cik=320193, file_type=FORM_TYPE, file_date='2016-10-26')
    assert File.accession_number.get_accession_number() == Acc_no1.get_accession_number()


def test_filing_construct_with_accession():
    number = '0001628280-16-020309'
    Acc_no1 = uc.AccessionNumber(number)
    File = uc.Filing.from_accession_number(Acc_no1)
    assert File.short_cik == '320193'


def test_filing_get_filing_document_url():
    #note the difference in cik in the url possibly due to change in firm name
    short_cik='51143'
    number = '0001628280-16-020309'
    doc_type = {'xbrl':'xbrl_instance_doc_url'}
    File = uc.Filing(short_cik, uc.AccessionNumber(number) )
    assert getattr(File.filing_metadata, doc_type['xbrl']) == 'https://www.sec.gov/Archives/edgar/data/320193/000162828016020309/aapl-20160924.xml'