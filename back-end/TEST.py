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
import uuid


# source = DWH.Source(
#     p_id='0a884f59-48b6-4afe-838e-57bdbbb9ec7c'
# )
#
# src_table_client=DWH.SourceTable(
#     p_name="crm_client",
#     p_source=source,
#     p_schema="crm"
# )
#
# src_attr_client_id=DWH.Attribute(
#     p_name="id",
#     p_pk=1,
#     p_type="queue_column",
#     p_datatype="varchar",
#     p_length=150,
#     p_attribute_type="queue_attr"
# )
#
# src_attr_pm_id=DWH.Attribute(
#     p_name="km_id",
#     p_pk=0,
#     p_type="queue_column",
#     p_datatype="varchar",
#     p_length=150
# )
#
# src_attr_pm_type=DWH.Attribute(
#     p_name="km_type",
#     p_pk=0,
#     p_type="queue_column",
#     p_datatype="varchar",
#     p_length=150
# )
#
# DWH.add_attribute(p_table=src_table_client,p_attribute=src_attr_client_id)
# DWH.add_attribute(p_table=src_table_client,p_attribute=src_attr_pm_id)
# DWH.add_attribute(p_table=src_table_client,p_attribute=src_attr_pm_type)
#
#
# entity_client = DWH.Entity(
#     p_name="Client",
#     p_source=[DWH.Source(p_id="0a884f59-48b6-4afe-838e-57bdbbb9ec7c")]
# )
#
# src_table_km=DWH.SourceTable(
#     p_name="crm_km",
#     p_source=source,
#     p_schema="crm"
# )
#
# km_src_attr_pm_id=DWH.Attribute(
#     p_name="id",
#     p_type="queue_column",
#     p_datatype="varchar",
#     p_length=150
# )
#
# DWH.add_attribute(p_table=src_table_km,p_attribute=km_src_attr_pm_id)
#
#
# entity_attr_client_id = DWH.Attribute(
#     p_name="client_id",
#     p_type='entity_column',
#     p_datatype="int",
#     p_pk=1,
#     p_source_attribute=[src_attr_client_id]
# )
#
# entity_attr_km_id=DWH.Attribute(
#     p_name="km_id",
#     p_type="entity_column",
#     p_datatype="int",
#     p_pk=0,
#     p_source_attribute=[src_attr_pm_id]
# )
#
# entity_attr_km_type=DWH.Attribute(
#     p_name="km_type",
#     p_type="entity_column",
#     p_datatype="int",
#     p_source_attribute=[src_attr_pm_type],
#     p_pk=0
# )
#
# DWH.add_attribute(p_table=entity_client,p_attribute=entity_attr_client_id)
# DWH.add_attribute(p_table=entity_client,p_attribute=entity_attr_km_id)
# DWH.add_attribute(p_table=entity_client,p_attribute=entity_attr_km_type)
#
# entity_km=DWH.Entity(
#     p_name="km"
# )
#
# km_entity_attr_km_id=DWH.Attribute(
#     p_name="id",
#     p_type="entity_column",
#     p_datatype="int",
#     p_source_attribute=[km_src_attr_pm_id]
# )
#
# DWH.add_attribute(p_table=entity_km,p_attribute=km_entity_attr_km_id)
#
# idmap_client=DWH.Idmap(
#     p_entity=entity_client,
#     p_source_attribute_nk=[src_attr_client_id]
# )
#
# idmap_km=DWH.Idmap(
#     p_entity=entity_km,
#     p_source_attribute_nk=[km_src_attr_pm_id]
# )
#
# anchor_client=DWH.Anchor(
#     p_entity=entity_client
# )
#
# attribute_client_id=DWH.AttributeTable(
#     p_entity=entity_client,
#     p_entity_attribute=entity_attr_client_id
# )
#
# tie_client=DWH.Tie(
#     p_entity=entity_client,
#     p_link_entity=entity_km,
#     p_source_table=[src_table_client],
#     p_entity_attribute=[entity_attr_km_id,entity_attr_km_type]
# )
#
# DWH.add_table(p_object=entity_client, p_table=src_table_client)
# DWH.add_table(p_object=idmap_client,p_table=src_table_client)

# print(
#     DWH.create_view_ddl(p_table=src_table_client)+"\n"
# )
# print(
#     DWH.create_view_ddl(p_table=tie_client)+"\n"
# )
# print(
#     DWH.create_view_ddl(p_table=idmap_km)
# )
# print(
#     DWH.get_tie_etl(p_tie=tie_client, p_etl_id=1)[0]
# )
# for j in entity_client.entity_attribute:
#     l_json=j.metadata_json
#     l_key=list(l_json.keys())
#     for i in l_key:
#         print(str(i)+":"+str(l_json.get(i)))
#     print("")
#
#     j.create_metadata()

# for i in src_table_client.source_attribute:
#     print(str(i.id)+" "+i.name)

l_json = """
{
    "entity":"CLIENTS",
    "description":"client entity",
    "attribute":[
            {
            "name":"id",
            "pk":1,
            "datatype":"int",
            "length":null,
            "scale":null,
            "link_entity":null,
            "description":"client id",
            "source":[
                    {
                    "source":"0a884f59-48b6-4afe-838e-57bdbbb9ec7c",
                    "schema":"crm",
                    "table":"client",
                    "column":"client_id"
                    }
                ]
        }
    ,
        {
            "name":"name",
            "pk":0,
            "datatype":"varchar",
            "length":100,
            "scale":null,
            "link_entity":null,
            "description":"client id",
            "source":[
                    {
                    "source":"0a884f59-48b6-4afe-838e-57bdbbb9ec7c",
                    "schema":"crm",
                    "table":"client",
                    "column":"client_id"
                    }
                ]
        }
        ]
    
}
"""

model=Model.Model(p_json=l_json)

model.create_model()

# for i in model.create_model():
#     # for j in i.source_attribute:
#     print(
#         i.metadata_json
#     )










