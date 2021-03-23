from typing import List, Union
from xialib.adaptor import Adaptor


class BigQueryAdaptor(Adaptor):
    def append_log_data(self, table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        pass

    def load_log_data(self, log_table_id: str, table_id: str, field_data: list, start_age: int, end_age: int):
        pass

    def append_normal_data(self, table_id: str, field_data: List[dict], data: List[dict], type: str, **kwargs):
        pass

    def upsert_data(self, table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        pass

    def purge_segment(self, table_id: str, meta_data: dict, segment_config: Union[dict, None]):
        pass

    def create_table(self, table_id: str, meta_data: dict, field_data: List[dict], type: str):
        pass

    def drop_table(self, table_id: str):
        pass

    def alter_column(self, table_id: str, old_field_line: dict, new_field_line: dict):
        pass

    def add_column(self, table_id: str, new_field_line: dict):
        pass
