from sec_edgar_downloader import UrlComponent as uc



# Accession

number = '0001193125-15-118890'
acc_num = uc.AccessionNumber(number)

def test_acc_default_values():
    new_acc_num = uc.AccessionNumber()
    assert new_acc_num.get_components() == {'short_cik': '999999999', 'year': '2099', 'annual_sequence': '999999'}

def test_acc_get_components():
    result = acc_num.get_components()
    assert result == {'short_cik': '1193125', 'year': '2015', 'annual_sequence': '118890'}

def test_acc_create_from_components():
    result = acc_num.get_components()
    new_acc_no = uc.AccessionNumber()
    new_acc_no.create_from_components(*result.values())
    assert acc_num.accession_number == number

def test_acc_verify_components_meet_requirements():
    assert acc_num.verify_components_meet_requirements() == True

def test_acc_get_nodash_accession_number():
    assert acc_num.get_nodash_accession_number() == number.replace('-','')


# Filing

def test_filing_create_from_accession_number():
    number = '0001628280-16-020309'
    errors = []

    accession_number = uc.AccessionNumber(number)
    file = uc.Filing(accession_number)
    cond1 = type(file) == uc.Filing
    cond2 = file.short_cik == '320193'
    cond3 = file.file_type == '10-K'
    if not cond1: errors.append('cond1')
    if not cond2: errors.append('cond2')
    if not cond3: errors.append('cond3')
    assert not errors

def test_filing_creation_from_file_details():
    FORM_TYPE = '10-K'
    number = '0001628280-16-020309'
    errors = []

    file = uc.Filing(short_cik='320193', file_type=FORM_TYPE, file_date='2016-10-26')
    cond1 = type(file) == uc.Filing
    cond2 = file.accession_number.get_accession_number_string() == number   
    if not cond1: errors.append('cond1')
    if not cond2: errors.append('cond2')
    assert not errors

def test_filing_get_filing_documents():
    #note the difference in cik in the url possibly due to change in firm name
    short_cik='51143'
    number = '0001628280-16-020309'
    doc_type = 'xbrl_instance_doc_url'
    file = uc.Filing(accession_number=uc.AccessionNumber(number), short_cik=short_cik)
    assert getattr(file, doc_type)[0] == 'https://www.sec.gov/Archives/edgar/data/320193/000162828016020309/aapl-20160924.xml'

def test_filing_create_key():
    number = '0001628280-16-020309'
    accession_number = uc.AccessionNumber(number)
    file = uc.Filing(accession_number)
    assert file.create_key() == '320193|0001628280-16-020309'

def test_filingget_file_record():
    number = '0001628280-16-020309'
    accession_number = uc.AccessionNumber(number)
    file = uc.Filing(accession_number)
    acc_no = file.get_file_record()['accession_number'].get_accession_number_string()
    assert acc_no == number

def test_filing_get_document_record_list():
    number = '0001628280-16-020309'
    accession_number = uc.AccessionNumber(number)
    file = uc.Filing(accession_number)
    record_list = file.get_document_record_list()
    assert record_list[0]['Seq'] == 1.0


# Firm
us_banks = [('JPMorgan Chase & CORP','JPM'),
            ('BANK OF AMERICA CORP','BAC'),
            ('WELLS FARGO & COMPANY/MN','WFC'),
            ('Citigroup','C')
            ]

def test_firm_creation_from_name():
    name = us_banks[0][0]
    firm = uc.Firm(name=name)
    cik = firm.get_firm_info()['cik']
    assert cik == '19617'

def test_firm_creation_from_ticker():
    ticker = us_banks[1][1]
    firm = uc.Firm(ticker=ticker)
    cik = firm.get_firm_info()['cik']
    assert cik == '70858'

def test_firm_get_firm_info():
    ticker = us_banks[2][1]
    firm = uc.Firm(ticker=ticker)
    firm_info = firm.get_firm_info()
    assert firm_info == {'cik': '72971', 'name': 'WELLS FARGO & COMPANY/MN', 'ticker': 'WFC', 'exchange': 'NYSE'}

def test_firm_get_ciks():
    ticker = us_banks[3][1]
    firm = uc.Firm(ticker=ticker)
    initialized_firms = firm.get_initialized_firms()
    assert type(initialized_firms) == list