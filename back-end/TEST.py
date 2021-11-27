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



##########
# stage
##########
# crm_client_id_stg_attr=DDL.Attribute(
#     p_attribute_name="client_id",
#     p_attribute_type="queue_attr",
#     p_attribute_id="103",
#     p_data_type={"datatype":"int"}
# )
#
# crm_client_fio_stg_attr=DDL.Attribute(
#     p_attribute_name="client_fio",
#     p_attribute_type="queue_attr",
#     p_attribute_id="101",
#     p_data_type={"datatype":"varchar","length":100}
# )
#
# crm_client_update_stg_attr=DDL.Attribute(
#     p_attribute_name="last_update",
#     p_attribute_type="update",
#     p_attribute_id="102",
#     p_data_type={"datatype":"timestamp"}
# )
#
# crm_client_etl_stg_attr=DDL.Attribute(
#     p_attribute_name="etl_id",
#     p_attribute_type="etl",
#     p_attribute_id="103",
#     p_data_type={"datatype":"bigint"}
# )
#
#
#
# crm_client_table=DDL.Table(
#     p_table_name="crm_client",
#     p_table_attrs=[crm_client_id_stg_attr,crm_client_fio_stg_attr,crm_client_update_stg_attr,crm_client_etl_stg_attr],
#     p_table_type="queue",
#     p_table_id="100",
#     p_source_schema="person",
#     p_source_table_name="client"
# )
# ##########
#
# ##########
# #idmap
# ##########
#
# idmap_source_client_nk_attr=DDL.SourceAttribute(
#     p_source_attribute_type="nk",
#     p_attribute=crm_client_id_stg_attr
# )
#
# idmap_source_table=DDL.SourceTable(
#     p_table=crm_client_table,
#     p_source_id="1",
#     p_source_table_type="queue",
#     p_source_table_attr=[idmap_source_client_nk_attr]
# )
#
# idmap_client_rk_attr=DDL.Attribute(
#     p_attribute_id="201",
#     p_attribute_name="client_rk",
#     p_attribute_type="rk",
#     p_data_type={"datatype":"bigint"}
# )
#
# idmap_client_nk_attr=DDL.Attribute(
#     p_attribute_id="202",
#     p_attribute_name="client_nk",
#     p_attribute_type="nk",
#     p_data_type={"datatype":"varchar","length":1000}
# )
#
# idmap_client_etl_attr=DDL.Attribute(
#     p_attribute_id="203",
#     p_attribute_name="etl_id",
#     p_attribute_type="etl",
#     p_data_type={"datatype":"bigint"}
# )
#
# idmap_table=DDL.Table(
#     p_table_id="200",
#     p_table_type="idmap",
#     p_table_attrs=[idmap_client_rk_attr,idmap_client_nk_attr,idmap_client_etl_attr],
#     p_table_name="idmap_client",
#     p_source_tables=[idmap_source_table]
# )
#
# #######
# #anchor
# #######
#
# anchor_client_rk=DDL.Attribute(
#     p_attribute_type="rk",
#     p_attribute_name="client_rk",
#     p_attribute_id="301",
#     p_data_type={"datatype":"bigint"}
# )
#
# anchor_client_source=DDL.Attribute(
#     p_attribute_type="source",
#     p_attribute_name="source_system_id",
#     p_attribute_id="302",
#     p_data_type={"datatype":"int"}
# )
#
# anchor_client_etl=DDL.Attribute(
#     p_attribute_type="etl",
#     p_attribute_name="etl_id",
#     p_attribute_id="303",
#     p_data_type={"datatype":"bigint"}
# )
#
# source_anchor_client_rk=DDL.SourceAttribute(
#     p_attribute=idmap_client_rk_attr,
#     p_source_attribute_type="rk"
# )
#
# source_anchor_table=DDL.SourceTable(
#     p_source_table_type="idmap",
#     p_source_table_attr=[source_anchor_client_rk],
#     p_table=idmap_table
# )
#
# anchor_table=DDL.Table(
#     p_source_tables=[source_anchor_table],
#     p_table_name="client_an",
#     p_table_type="anchor",
#     p_table_id="300",
#     p_table_attrs=[anchor_client_rk,anchor_client_source,anchor_client_etl]
# )
#
# #########
# #Attribute
# #########
#
# attribute_table_client_rk=DDL.Attribute(
#     p_attribute_id="401",
#     p_attribute_name="client_rk",
#     p_attribute_type="rk",
#     p_data_type={"datatype":"bigint"}
# )
#
# attribute_table_client_fio=DDL.Attribute(
#     p_attribute_id="402",
#     p_attribute_name="client_fio",
#     p_attribute_type="value",
#     p_data_type={"datatype":"varchar","length":100}
# )
#
# attribute_table_client_from=DDL.Attribute(
#     p_attribute_id="402",
#     p_attribute_name="from_dttm",
#     p_attribute_type="from",
#     p_data_type={"datatype":"timestamp"}
# )
#
# attribute_table_client_to=DDL.Attribute(
#     p_attribute_id="403",
#     p_attribute_name="to_dttm",
#     p_attribute_type="to",
#     p_data_type={"datatype":"timestamp"}
# )
#
# attribute_table_client_etl=DDL.Attribute(
#     p_attribute_id="404",
#     p_attribute_name="etl",
#     p_attribute_type="etl",
#     p_data_type={"datatype":"bigint"}
# )
#
# source_attribute_client_rk=DDL.SourceAttribute(
#     p_attribute=idmap_client_rk_attr,
#     p_source_attribute_type="rk"
# )
#
# source_attribute_client_nk=DDL.SourceAttribute(
#     p_attribute=idmap_client_nk_attr,
#     p_source_attribute_type="rk"
# )
#
# source_attribute_idmap_table=DDL.SourceTable(
#     p_source_table_type="idmap",
#     p_source_table_attr=[source_anchor_client_rk,source_attribute_client_nk],
#     p_table=idmap_table
# )
#
#
# source_attribute_client_id=DDL.SourceAttribute(
#     p_source_attribute_type="nk",
#     p_attribute=crm_client_id_stg_attr
# )
#
# source_attribute_client_fio=DDL.SourceAttribute(
#     p_source_attribute_type="value",
#     p_attribute=crm_client_fio_stg_attr
# )
#
# source_attribute_client_update=DDL.SourceAttribute(
#     p_source_attribute_type="from",
#     p_attribute=crm_client_update_stg_attr
# )
#
# source_attribute_table=DDL.SourceTable(
#     p_table=crm_client_table,
#     p_source_table_attr=[source_attribute_client_fio,source_attribute_client_update,source_attribute_client_id],
#     p_source_table_type="queue",
#     p_source_id="1"
# )
#
# attribute_table=DDL.Table(
#     p_table_id="400",
#     p_table_name="client_fio_attr",
#     p_table_type="attribute",
#     p_source_tables=[source_attribute_table,source_attribute_idmap_table],
#     p_table_attrs=[attribute_table_client_rk,attribute_table_client_etl,attribute_table_client_fio,attribute_table_client_from,attribute_table_client_to]
# )
#
#
# #########
# #Tie
# #########
#
# tie_client_rk=DDL.Attribute(
#     p_attribute_id="501",
#     p_attribute_type="rk",
#     p_attribute_name="client_rk",
#     p_data_type={"datatype":"bigint"}
# )
#
# tie_client_link_rk=DDL.Attribute(
#     p_attribute_id="502",
#     p_attribute_type="link_rk",
#     p_attribute_name="client_link_rk",
#     p_data_type={"datatype":"bigint"}
# )
#
# tie_client_to=DDL.Attribute(
#     p_attribute_id="503",
#     p_attribute_type="to",
#     p_attribute_name="to",
#     p_data_type={"datatype":"bigint"}
# )
#
# tie_client_from=DDL.Attribute(
#     p_attribute_id="504",
#     p_attribute_type="from",
#     p_attribute_name="from",
#     p_data_type={"datatype":"bigint"}
# )
#
# tie_client_etl=DDL.Attribute(
#     p_attribute_id="505",
#     p_attribute_type="etl",
#     p_attribute_name="etl",
#     p_data_type={"datatype":"bigint"}
# )
#
# source_tie_client_rk=DDL.SourceAttribute(
#     p_attribute=idmap_client_rk_attr,
#     p_source_attribute_type="rk"
# )
#
# source_tie_client_link_nk=DDL.SourceAttribute(
#     p_attribute=idmap_client_nk_attr,
#     p_source_attribute_type="nk"
# )
#
# source_tie_idmap=DDL.SourceTable(
#     p_source_table_type="idmap",
#     p_source_table_attr=[source_tie_client_rk,source_tie_client_link_nk],
#     p_table=idmap_table
# )
#
# source_tie_link_idmap=DDL.SourceTable(
#     p_source_table_type="link_idmap",
#     p_source_table_attr=[source_tie_client_rk,source_tie_client_link_nk],
#     p_table=idmap_table
# )
#
# source_tie_client_rk_stg=DDL.SourceAttribute(
#     p_source_attribute_type="nk",
#     p_attribute=crm_client_id_stg_attr
# )
#
# source_tie_client_link_rk_stg=DDL.SourceAttribute(
#     p_source_attribute_type="link_nk",
#     p_attribute=crm_client_id_stg_attr
# )
#
# source_tie_client_link_update_stg=DDL.SourceAttribute(
#     p_source_attribute_type="update",
#     p_attribute=crm_client_update_stg_attr
# )
#
# source_tie_stg=DDL.SourceTable(
#     p_table=crm_client_table,
#     p_source_table_type="queue",
#     p_source_table_attr=[source_tie_client_rk_stg,source_tie_client_link_rk_stg, source_tie_client_link_update_stg],
#     p_source_id="1"
# )
#
# tie_table=DDL.Table(
#     p_table_id="500",
#     p_table_name="client_tie",
#     p_table_attrs=[tie_client_etl,tie_client_from,tie_client_link_rk,tie_client_rk,tie_client_to],
#     p_source_tables=[source_tie_idmap, source_tie_link_idmap, source_tie_stg],
#     p_table_type="tie"
# )
#
#
#
# #########
# #ETL
# #########
# table_etl=DDL.ETL(
#     p_table=crm_client_table
# )
#########


# print(
#     crm_client_table.attribute_name_sql(p_attribute_type=["queue_attr"])
# )

# print(
#     table_etl.get_data_extract_script()
# )


x = Metadata.MetaObject(
    p_type="source",
    p_attrs={
        'server': '77.37.162.204',
        'database': 'CRM',
        'user': 'AnchorBI',
        'password': 'AnchorBI',
        'name': 'Test source',
        'type': 'MSSQL',
        'port': 1433,
        'deleted': 0,
        'source_id':2
    },
    p_uuid='0f884f59-48b6-4afe-838e-57bdbbb9ec7c'
)


y = Source.Source(
    p_name='test18',
    p_database='CRM',
    p_server='77.37.162.204',
    p_user='AnchorBI',
    p_password='AnchorBI',
    p_type='mssql',
    p_port=1433,
    p_desc='Test source',
    p_id='cbbf1a60-b5d0-481f-ad59-f92d6f452b89'

)


print(
    y.sql_exec("select 1")
)
