import os
import json
import pytest
from datetime import datetime, timedelta
import random
import string
from google.cloud import bigquery
from xialib_bigquery.bigquery_adaptor import BigQueryAdaptor

ddl_table_id = "..test.simple_person_ddl"
rand = ''.join(random.choice(string.ascii_lowercase) for i in range(3))
std_table_id = "..test.simple_person_std_" + rand
aged_table_id = "..test.simple_person_aged_" + rand
expires_at = (datetime.now() + timedelta(minutes=10)).timestamp()

with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
    field_data = json.load(fp)

@pytest.fixture(scope='module')
def adaptor():
    conn = bigquery.Client()
    adaptor = BigQueryAdaptor(db=conn)
    adaptor.drop_table(ddl_table_id)
    adaptor.drop_table(std_table_id)
    adaptor.drop_table(aged_table_id)
    yield adaptor


def test_ddl(adaptor: BigQueryAdaptor):
    assert adaptor.create_table(ddl_table_id, adaptor.log_table_meta, field_data, "aged")
    adaptor.support_add_column = False
    assert not adaptor.add_column(ddl_table_id, adaptor._age_field)
    adaptor.support_add_column = True
    assert adaptor.add_column(ddl_table_id, adaptor._seq_field)
    adaptor.support_alter_column = False
    assert not adaptor.alter_column(ddl_table_id, {'type_chain': ['char', 'c_8']}, {'type_chain': ['char', 'c_9']})
    adaptor.support_alter_column = True
    assert adaptor.alter_column(ddl_table_id, {'type_chain': ['char', 'c_8']}, {'type_chain': ['char', 'c_9']})

def test_std_case(adaptor: BigQueryAdaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
        for line in data_02:
            line["_SEQ"] = datetime.now().strftime('%Y%m%d%H%M%S%f')
    assert adaptor.create_table(std_table_id, {"expires_at": expires_at}, field_data, "normal")
    assert adaptor.append_normal_data(std_table_id, field_data, data_02, "normal")

def test_aged_case(adaptor: BigQueryAdaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
        for line in data_02:
            line["_AGE"] = line["id"] // 10 + 2
            line["_NO"] = line["id"] % 10 + 1
    assert adaptor.create_table(aged_table_id, adaptor.log_table_meta, field_data, "aged")
    assert adaptor.append_log_data(aged_table_id, field_data, data_02)
