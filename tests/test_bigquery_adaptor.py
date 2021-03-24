import os
import json
import pytest
import time
from google.cloud import bigquery
from xialib_bigquery.bigquery_adaptor import BigQueryAdaptor

ddl_table_id = "..test.simple_person_ddl"
table_id = "..test.simple_person"

with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
    field_data = json.load(fp)

@pytest.fixture(scope='module')
def adaptor():
    conn = bigquery.Client()
    adaptor = BigQueryAdaptor(db=conn)
    adaptor.drop_table(ddl_table_id)
    yield adaptor


def test_ddl(adaptor: BigQueryAdaptor):
    assert adaptor.create_table(ddl_table_id, adaptor.log_table_meta, field_data, "aged")
    assert adaptor.add_column(ddl_table_id, adaptor._seq_field)
    assert adaptor.alter_column(ddl_table_id, {'type_chain': ['char', 'c_8']}, {'type_chain': ['char', 'c_9']})

def test_simple(adaptor: BigQueryAdaptor):
    """
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
        for line in data_02:
            line["_AGE"] = line["id"] // 10 + 2
            line["_NO"] = line["id"] % 10 + 1
    assert adaptor.create_table(table_id, adaptor.log_table_meta, field_data, "aged")
    assert adaptor.append_log_data(table_id, field_data, data_02)
    """