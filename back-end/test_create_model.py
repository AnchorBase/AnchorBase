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

source = DWH.Source(
    p_id='0a884f59-48b6-4afe-838e-57bdbbb9ec7c'
)

src_table_client=DWH.SourceTable(
    p_name="crm_client",
    p_source=source,
    p_schema="crm"
)

src_attr_client_id=DWH.Attribute(
    p_name="id",
    p_type="queue_column",
    p_datatype="varchar",
    p_length=150
)

src_attr_pm_id=DWH.Attribute(
    p_name="km_id",
    p_type="queue_column",
    p_datatype="varchar",
    p_length=150
)

DWH.add_attribute(p_table=src_table_client,p_attribute=src_attr_client_id)
DWH.add_attribute(p_table=src_table_client,p_attribute=src_attr_pm_id)


entity_client = DWH.Entity(
    p_name="Client"
)

src_table_km=DWH.SourceTable(
    p_name="crm_km",
    p_source=source,
    p_schema="crm"
)

km_src_attr_pm_id=DWH.Attribute(
    p_name="id",
    p_type="queue_column",
    p_datatype="varchar",
    p_length=150
)

DWH.add_attribute(p_table=src_table_km,p_attribute=km_src_attr_pm_id)


entity_attr_client_id = DWH.Attribute(
    p_name="client_id",
    p_type='entity_column',
    p_datatype="int",
    p_source_attribute=[src_attr_client_id]
)

entity_attr_km_id=DWH.Attribute(
    p_name="km_id",
    p_type="entity_column",
    p_datatype="int",
    p_source_attribute=[src_attr_pm_id]
)

DWH.add_attribute(p_table=entity_client,p_attribute=entity_attr_client_id)
DWH.add_attribute(p_table=entity_client,p_attribute=entity_attr_km_id)

entity_km=DWH.Entity(
    p_name="km"
)

km_entity_attr_km_id=DWH.Attribute(
    p_name="id",
    p_type="entity_column",
    p_datatype="int",
    p_source_attribute=[km_src_attr_pm_id]
)

DWH.add_attribute(p_table=entity_km,p_attribute=km_entity_attr_km_id)

idmap_client=DWH.Idmap(
    p_entity=entity_client,
    p_source_attribute_nk=[src_attr_client_id]
)

idmap_km=DWH.Idmap(
    p_entity=entity_km,
    p_source_attribute_nk=[km_src_attr_pm_id]
)

anchor_client=DWH.Anchor(
    p_entity=entity_client
)

attribute_client_id=DWH.AttributeTable(
    p_entity=entity_client,
    p_entity_attribute=entity_attr_client_id
)

tie_client=DWH.Tie(
    p_entity=entity_client,
    p_link_entity=entity_km,
    p_source_table=[src_table_client],
    p_entity_attribute=[entity_attr_km_id],
    p_link_entity_attribute=[km_src_attr_pm_id]
)

DWH.add_attribute(p_table=src_table_client,p_attribute=src_attr_client_id)

DWH.add_table(p_object=entity_client, p_table=src_table_client)


print(
    DWH.create_table_ddl(p_table=tie_client)+"\n"
)
print(
    DWH.create_view_ddl(p_table=tie_client)
)
print(
    DWH.get_tie_etl(p_tie=tie_client, p_etl_id=1)[0]
)




