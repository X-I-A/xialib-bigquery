import os
import json
import time
import pytest
from datetime import datetime, timedelta
import random
import string
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from xialib_bigquery.bigquery_adaptor import BigQueryAdaptor

ddl_table_id = "..test.simple_person_ddl"
rand = ''.join(random.choice(string.ascii_lowercase) for i in range(3))
std_table_id = "..test.simple_person_std_" + rand
aged_table_id = "..test.simple_person_aged"
aged_log_table_id = "..test.simple_person_aged_" + rand
expires_at = (datetime.now() + timedelta(minutes=10)).timestamp()

segment_0 = {'id': '0', 'field_name': 'height', 'type_chain': ['int'], 'null': True}
segment_1 = {'id': '1', 'field_name': 'height', 'type_chain': ['int'], 'list': [150, 151, 152, 153]}
segment_2 = {'id': '2', 'field_name': 'height', 'type_chain': ['int'], 'min': 160, 'max': 169}
segment_3 = {'id': '3', 'field_name': 'height', 'type_chain': ['int'], 'default': 170}

count_sql = "SELECT COUNT(id) FROM {}"
select_sql = "SELECT * FROM {} WHERE {}"

with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
    field_data = json.load(fp)

@pytest.fixture(scope='module')
def adaptor():
    conn = bigquery.Client()
    adaptor = BigQueryAdaptor(db=conn)
    adaptor.drop_table(ddl_table_id)
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
    job = adaptor.connection.query(count_sql.format((adaptor._get_table_id(std_table_id))))
    assert list(job.result())[0][0] == 1000

def test_aged_case(adaptor: BigQueryAdaptor):
    table_meta = {
        "partition": {"birthday": {"type": "time", "criteria": "month"}},
        "cluster": {"gender": {}},
    }
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
        for line in data_02:
            line["_AGE"] = line["id"] // 10 + 2
            line["_NO"] = line["id"] % 10 + 1
            line["_OP"] = ""
    log_meta = adaptor.log_table_meta.copy()
    log_meta.update({"expires_at": expires_at})
    assert adaptor.create_table(aged_table_id, table_meta, field_data, "raw")
    assert adaptor.create_table(aged_log_table_id, log_meta, field_data, "aged")
    assert adaptor.append_log_data(aged_log_table_id, field_data, data_02)
    assert adaptor.load_log_data(aged_log_table_id, aged_table_id, field_data, table_meta, 2, 102)
    delete_list = [
        {"_AGE": 103, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'I', "_NO": 1},
        {"_AGE": 103, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'D', "_NO": 2}
    ]
    update_list = [{"_AGE": 104, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "birthday": "1971-05-25",
                    "city": "Paris", "_OP": 'U'}]
    time.sleep(2)
    table_meta["cluster"] = {"first_name": {}}
    assert adaptor.append_log_data(aged_log_table_id, field_data, delete_list + update_list)
    assert adaptor.load_log_data(aged_log_table_id, aged_table_id, field_data, table_meta, 103, 104)
    job = adaptor.connection.query(count_sql.format((adaptor._get_table_id(aged_table_id))))
    assert list(job.result())[0][0] == 999
    job = adaptor.connection.query(select_sql.format((adaptor._get_table_id(aged_table_id)), "id <= 2"))
    values = [dict(row)["city"] for row in job.result()]
    assert "Paris" in values
    assert len(values) == 1

    assert not adaptor.upsert_data(aged_table_id, field_data, delete_list + update_list)
    assert adaptor.purge_segment(aged_table_id, {}, segment_0)
    assert adaptor.purge_segment(aged_table_id, {}, segment_1)
    assert adaptor.purge_segment(aged_table_id, {}, segment_2)
    assert adaptor.purge_segment(aged_table_id, {}, segment_3)