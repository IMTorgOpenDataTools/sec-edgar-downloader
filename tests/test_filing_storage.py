from sec_edgar_downloader import FilingStorage as fs
from sec_edgar_downloader import UrlComponent as uc


from pathlib import Path

dir_path = Path('tests/TEMP')
fstorage = fs.FilingStorage(dir_path)
#TODO: fix this entire file before use

def test_dump_to_pickle():
    fstorage.dump_to_pickle()
    pkl_path = dir_path / fstorage._file_name
    assert pkl_path.is_file() == True

def test_load_from_pickle():
    fstorage.load_from_pickle()
    cond = hasattr(fstorage, '__FilingSet')
    assert cond == True

def test_add_record():
    accession_number = ''
    file = uc.Filing(accession_number)
    fstorage.add_record(record=file, rec_lst='')
    assert True == True

def test_get_all_records():
    dict_of_docs = fstorage.get_all_records(mode='file')
    assert dict_of_docs == {}

def test_get_record():
    key = ''
    rec = fstorage.get_record(idx_or_key=key)
    assert rec == {}

def test_get_document_in_record():
    doc = fstorage.get_document_in_record(idx_or_list='')
    assert doc == {}

def test_modify_record():
    orig_record = ''
    new_record =''
    fstorage.modify_record(orig_record, new_record)
    assert True == True

def test_modify_document_in_record():
    file_key = ''
    orig_document = ''
    new_document = ''
    fstorage.modify_document_in_record(file_key, orig_document, new_document)
    assert True == True

def test_get_list():
    lst = fstorage.get_list()
    assert True == True

def test_get_dataframe():
    df = fstorage.get_dataframe(mode='file')
    assert True == True

def test_sync_with_filesystem():
    assert True == True