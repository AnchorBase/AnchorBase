# coding=utf-8
#!path/to/interpretter
#!/usr/bin/python
import DWH
import Model
import Postgresql
import Source
import API
import Metadata
import Constants
import shlex
import ConsoleWork
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
                    "column":"client_name"
                    }
                ]
        }
        ]

}
"""

l_json2="""
{
    "entity":"orders",
    "description":"order entity",
    "attribute":[
            {
            "name":"order_id",
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
                    "table":"orders",
                    "column":"order_id"
                    }
                ]
        }
    ,
        {
            "name":"client_id",
            "pk":0,
            "datatype":"int",
            "length":null,
            "scale":null,
            "link_entity":"aeccb947-8b27-4eb9-a21d-437d375730a6",
            "description":"client id",
            "source":[
                    {
                    "source":"0a884f59-48b6-4afe-838e-57bdbbb9ec7c",
                    "schema":"crm",
                    "table":"orders",
                    "column":"client_id"
                    }
                ]
        }
        ]

}
"""

# model=Model.Model(p_json=l_json)
#
# model.create_model()





# l_source_table=DWH.SourceTable(p_id="9363affc-5d1d-4930-a373-95b082c84af9")
# l_source_table_client=DWH.SourceTable(p_id="be6b5819-0287-47e4-a0a5-1ca234471400")
#
# l_idmap=DWH.Idmap(
#     p_id="18efd259-7ee3-4203-9548-04cae36e4f76"
# )
#
# l_idmap_order=DWH.Idmap(
#     p_id="f39f042d-5fff-41bf-ae42-17fcc4f9ac7d"
# )
#
# l_order_idmap=DWH.Idmap(p_id="19eeeb8e-f1a9-45c3-86a7-6d818a61855a")
#
# l_anchor=DWH.Anchor(p_id="e0fa0d1b-9d54-4874-8d15-8581760d9dfc")
#
# l_anchor_order=DWH.Anchor(p_id="971d334b-9a51-440c-8ceb-b294f0eb639d")
#
# l_attribute_id=DWH.AttributeTable(p_id="caf6f9b8-30a4-428e-9e76-1f49253017ce")
#
# l_attribute_name=DWH.AttributeTable(p_id="3f53d815-af86-4cd2-9615-d36d2dc0f517")
#

# l_entity=DWH.Entity(p_id="95aa8940-a8eb-4325-adec-daf3ffe6acea")
# l_enntity_attribute=DWH.Attribute(p_id="6cefcf44-ec9a-4e49-8c09-12a72b3f20f2", p_type="entity_column")
# l_tie=DWH.Tie(p_id="9fd90d40-31ad-4e7d-aa50-4263d588d6af")


# l_entity_column=DWH.Attribute(p_id="2788efbe-419a-484c-b77c-e5b275901896", p_type="entity_column")
#

# l_source=Source.Source(p_id="0a884f59-48b6-4afe-838e-57bdbbb9ec7c")


txt="get_source  -name  'test source'"

print(
    uuid.UUID("sfsd")
)


# l_job=DWH.Job(
# )
# l_job.start_job()

# l_entity=DWH.Entity(
#     p_id="b115babb-7925-4020-bf7d-aaa4c8519b84"
# )
#
# print(
#     l_entity.get_entity_function()
# )











