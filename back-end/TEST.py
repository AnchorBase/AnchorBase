# coding=utf-8
import Source
import Metadata
import Model
import DataLoad
import Driver
import ETL
import DDL
import DWH
import Postgresql
from platform import system
import MSSQL
import copy


entity_client = DWH.Entity(
    p_name="Client"
)

entity_attr_client_id = DWH.Attribute(
    p_name="client_id",
    p_type='entity_column'
)

entity_attr_client_name = DWH.Attribute(
    p_name="client_name",
    p_type='entity_column'
)

DWH.add_attribute(p_table=entity_client,p_attribute=entity_attr_client_id)
DWH.add_attribute(p_table=entity_client,p_attribute=entity_attr_client_name)

src_table_client=DWH.SourceTable(
    p_name="crm_client"
)

src_attr_client_id=DWH.Attribute(
    p_name="id",
    p_type="queue_column"
)

DWH.add_attribute(p_table=src_table_client,p_attribute=src_attr_client_id)

print(
    src_table_client.source_attribute[0].name
)




