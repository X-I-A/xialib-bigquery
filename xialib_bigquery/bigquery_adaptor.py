import json
from typing import List, Union
from datetime import datetime, timedelta
from google.cloud import bigquery
import google.auth
from google.api_core.exceptions import Conflict, BadRequest
from xialib.adaptor import Adaptor


class BigQueryAdaptor(Adaptor):
    support_add_column = True
    support_alter_column = True

    _age_field = {'field_name': '_AGE', 'key_flag': False, 'type_chain': ['int', 'ui_8'],
                  'format': None, 'encode': None, 'default': 0}
    _seq_field = {'field_name': '_SEQ', 'key_flag': False, 'type_chain': ['char', 'c_20'],
                  'format': None, 'encode': None, 'default': '0'*20}
    _no_field = {'field_name': '_NO', 'key_flag': False, 'type_chain': ['int', 'ui_8'],
                 'format': None, 'encode': None, 'default': 0}
    _op_field = {'field_name': '_OP', 'key_flag': False, 'type_chain': ['char', 'c_1'],
                 'format': None, 'encode': None, 'default': ''}
    _ts_field = {'field_name': '_DT', 'key_flag': False, 'type_chain': ['datetime'],
                 'format': None, 'encode': None, 'default': ''}

    table_extension = {
        "raw": [],
        "aged": [_age_field, _no_field, _op_field, _ts_field],
        "normal": [_seq_field, _no_field, _op_field],
    }

    type_dict = {
        'NULL': ['null'],
        'INT64': ['int'],
        'FLOAT64': ['real'],
        'STRING': ['char'],
        'BYTES': ['blob'],
        'DATETIME': ['datetime']
    }

    log_table_meta = {
        "partition": {"_DT": {"type": "time", "criteria": "hour"}},
        "cluster": {"_AGE": {}},
    }

    def __init__(self, db: bigquery.Client, location: str = 'EU', **kwargs):
        super().__init__(**kwargs)
        if not isinstance(db, bigquery.Client):
            self.logger.error("connection must a big-query client", extra=self.log_context)
            raise TypeError("XIA-010005")
        else:
            self.connection = db
        self.location = location
        self.default_project = google.auth.default()[1]

    def _escape_column_name(self, old_name: str) -> str:
        """A column name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_),
        and it must start with a letter or underscore. The maximum column name length is 128 characters.
        A column name cannot use any of the following prefixes: _TABLE_, _FILE_, _PARTITION
        """
        better_name = old_name.translate({ord(c): "_" for c in r"!@#$%^&*()[]{};:,./<>?\|`~-=+"})
        if better_name[0].isdigit():
            better_name = '_' + better_name
        if better_name.upper().startswith('_TABLE_') or \
            better_name.upper().startswith('_FILE_') or \
            better_name.upper().startswith('_PARTITION'):
            better_name = '_' + better_name
        if len(better_name) > 128:
            better_name = better_name[:128]
        return better_name

    def _get_field_type(self, type_chain: list):
        for type in reversed(type_chain):
            for key, value in self.type_dict.items():
                if type in value:
                    return key
        self.logger.error("{} Not supported".format(json.dumps(type_chain)), extra=self.log_context)  # pragma: no cover
        raise TypeError("XIA-000020")  # pragma: no cover

    def _get_table_schema(self, field_data: List[dict]) -> List[dict]:
        schema = list()
        for field in field_data:
            schema_field = {'name': self._escape_column_name(field['field_name']),
                            'description': field.get('description', '')}
            if field.get('key_flag', False):
                schema_field['mode'] = 'REQUIRED'
            schema_field['type'] = self._get_field_type(field['type_chain'])
            schema.append(schema_field.copy())
        return schema

    def _get_dataset_id(self, table_id) -> str:
        table_path = table_id.split(".")
        project_id = table_path[-3] if len(table_path) >= 3 and table_path[-3] else self.default_project
        dataset_name = table_path[-2] if len(table_path) >= 2 and table_path[-2] else 'xia_default'
        dataset_id = '.'.join([project_id, dataset_name])
        return dataset_id

    def _get_table_id(self, table_id) -> str:
        dataset_id = self._get_dataset_id(table_id)
        bq_table_id = '.'.join([dataset_id, table_id.split('.')[-1]])
        return bq_table_id

    def append_log_data(self, table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        for i in range(((len(data) - 1) // 10000) + 1):
            start, end = i * 10000, (i + 1) * 10000
            load_data = []
            for line in data[start: end]:
                line["_DT"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                load_data.append({self._escape_column_name(k): v for k, v in line.items()})
            try:
                errors = self.connection.insert_rows_json(self._get_table_id(table_id), load_data)
            except BadRequest as e:  # pragma: no cover
                return False  # pragma: no cover
            if errors == []:
                continue
            else:  # pragma: no cover
                self.logger.error("Insert {} Error: {}".format(table_id, errors), extra=self.log_context)
                return False
        return True

    def load_log_data(self, log_table_id: str, table_id: str, field_data: list, meta_data: dict,
                      start_age: int, end_age: int):
        pass  # pragma: no cover

    def append_normal_data(self, table_id: str, field_data: List[dict], data: List[dict], type: str, **kwargs):
        for i in range(((len(data) - 1) // 10000) + 1):
            start, end = i * 10000, (i + 1) * 10000
            load_data = []
            for line in data[start: end]:
                load_data.append({self._escape_column_name(k): v for k, v in line.items()})
            try:
                errors = self.connection.insert_rows_json(self._get_table_id(table_id), load_data)
            except BadRequest as e:  # pragma: no cover
                return False  # pragma: no cover
            if errors == []:
                continue
            else:  # pragma: no cover
                self.logger.error("Insert {} Error: {}".format(table_id, errors), extra=self.log_context)
                return False
        return True

    def upsert_data(self, table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        pass  # pragma: no cover

    def purge_segment(self, table_id: str, meta_data: dict, segment_config: Union[dict, None]):
        pass  # pragma: no cover

    def create_table(self, table_id: str, meta_data: dict, field_data: List[dict], type: str):
        # Dataset level operation
        dataset = bigquery.Dataset(self._get_dataset_id(table_id))
        dataset.location = self.location
        try:
            self.connection.create_dataset(dataset, timeout=30)
        except Conflict as e:
            self.logger.info("Dataset already exists, donothing", extra=self.log_context)

        # Table Schema Definition
        field_list = field_data.copy()
        field_list.extend(self.table_extension.get(type))
        schema = self._get_table_schema(field_list)
        table = bigquery.Table(self._get_table_id(table_id), schema=schema)
        # Table Clustering
        if "cluster" in meta_data or "segment" in meta_data:
            partition_field = meta_data.get("segment", {}).get("field_name", "")
            clustering_fields = [partition_field] if partition_field else []
            clustering_fields += list(meta_data.get("cluster", {}))
            table.clustering_fields = clustering_fields
        # Table Partitioning
        for field_name, field_config in meta_data.get("partition", {}).items():
            if field_config.get("type", "") == "time" and field_config.get("criteria", "") in ["hour", "month", "day"]:
                table.time_partitioning = bigquery.table.TimePartitioning(
                    type_=field_config["criteria"].upper(),
                    field=field_name,
                    expiration_ms=None
                )
                break
        # Table Expiration
        if isinstance(meta_data.get("expires_at", 0), (float, int)) and \
            meta_data.get("expires_at", 0) > datetime.now().timestamp():
            table.expires = datetime.fromtimestamp(meta_data["expires_at"])
        try:
            table = self.connection.create_table(table, True, timeout=30)
            self.logger.info("Created table {}".format(table.table_id), extra=self.log_context)
            return True
        except BadRequest as e:  # pragma: no cover
            self.logger.error("Table Creation Failed: {}".format(e), extra=self.log_context)
            return False

    def drop_table(self, table_id: str):
        try:
            self.connection.delete_table(self._get_table_id(table_id), not_found_ok=True, timeout=30)
        except Exception as e:  # pragma: no cover
            self.logger.error("Table drop failed: {}".format(e), extra=self.log_context)
            return False

    def alter_column(self, table_id: str, old_field_line: dict, new_field_line: dict):
        if not self.support_alter_column:
            return False
        old_type = self._get_field_type(old_field_line['type_chain'])
        new_type = self._get_field_type(new_field_line['type_chain'])
        return True if old_type == new_type else False

    def add_column(self, table_id: str, new_field_line: dict):
        if not self.support_add_column:
            return False
        field_list = [new_field_line]
        table = self.connection.get_table(self._get_table_id(table_id))
        original_schema = table.schema
        new_schema = original_schema[:]
        new_schema.extend(self._get_table_schema(field_list))
        table.schema = new_schema
        try:
            table = self.connection.update_table(table, ["schema"])
            self.logger.info("Table Column {} is added".format(new_field_line), extra=self.log_context)
            return True if len(table.schema) == len(original_schema) + 1 == len(new_schema) else False
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover

