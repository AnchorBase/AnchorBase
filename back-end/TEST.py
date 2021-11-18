import Source
import Metadata
import Model
import DataLoad
import Driver
import ETL
import DDL

# idmap
idmap_client_rk = DDL.Attribute(
    p_attribute_type="rk",
    p_attribute_id="1",
    p_attribute_name="client_rk",
    p_data_type={"datatype":"int"}
)

idmap_client_nk = DDL.Attribute(
    p_attribute_type="nk",
    p_attribute_id="2",
    p_attribute_name="client_nk",
    p_data_type={"datatype":"int"}
)

idmap_client_etl = DDL.Attribute(
    p_attribute_type="etl",
    p_attribute_id="3",
    p_attribute_name="etl",
    p_data_type={"datatype":"int"}
)

idmap_client_table = DDL.Table(
    p_table_name="IdmapClient",
    p_table_attrs=[idmap_client_rk,idmap_client_nk,idmap_client_etl],
    p_table_type="idmap",
    p_table_id="1"
)

#queue

queue_client_nk = DDL.Attribute(
    p_attribute_id="11",
    p_attribute_type="nk",
    p_attribute_name="s_client_nk",
    p_data_type={"datatype":"int"}
)

queue_client_nk2 = DDL.Attribute(
    p_attribute_id="21",
    p_attribute_type="nk",
    p_attribute_name="s_client_nk",
    p_data_type={"datatype":"int"}
)

queue_client_fio = DDL.Attribute(
    p_attribute_id="12",
    p_attribute_type="queue_attr",
    p_attribute_name="s_fio",
    p_data_type={"datatype":"varchar", "length":10}
)

queue_client_update = DDL.Attribute(
    p_attribute_id="13",
    p_attribute_type="update",
    p_attribute_name="s_update",
    p_data_type={"datatype":"timestamp"}
)

queue_client_etl = DDL.Attribute(
    p_attribute_id="14",
    p_attribute_type="etl",
    p_attribute_name="s_etl",
    p_data_type={"datatype":"int"}
)

queue_client_table = DDL.Table(
    p_table_id="10",
    p_table_type="queue",
    p_table_attrs=[queue_client_nk,queue_client_fio,queue_client_update,queue_client_etl],
    p_table_name="queue_client_table"
)

queue_client_table2 = DDL.Table(
    p_table_id="20",
    p_table_type="queue",
    p_table_attrs=[queue_client_nk,queue_client_fio,queue_client_update,queue_client_etl],
    p_table_name="queue_client_table"
)
# источники

source_idmap_client_rk = DDL.SourceAttribute(
    p_attribute=idmap_client_rk,
    p_source_attribute_type="rk"
)

source_idmap_client_nk = DDL.SourceAttribute(
    p_attribute=idmap_client_nk,
    p_source_attribute_type="nk"
)

source_idmap_client_table = DDL.SourceTable(
    p_table=idmap_client_table,
    p_source_table_attr=[source_idmap_client_rk,source_idmap_client_nk],
    p_source_table_type="idmap"
)

source_queue_client_nk = DDL.SourceAttribute(
    p_attribute=queue_client_nk,
    p_source_attribute_type="nk"
)

source_queue_client_nk2 = DDL.SourceAttribute(
    p_attribute=queue_client_nk2,
    p_source_attribute_type="nk"
)

source_queue_client_fio = DDL.SourceAttribute(
    p_attribute=queue_client_fio,
    p_source_attribute_type="queue_attr"
)

source_queue_client_update = DDL.SourceAttribute(
    p_attribute=queue_client_update,
    p_source_attribute_type="update"
)

source_queue_client_table = DDL.SourceTable(
    p_table=queue_client_table,
    p_source_table_attr=[source_queue_client_nk,source_queue_client_fio,source_queue_client_update],
    p_source_table_type="queue",
    p_source_id="1"
)


source_queue_client_table2 = DDL.SourceTable(
    p_table=queue_client_table2,
    p_source_table_attr=[source_queue_client_nk2,source_queue_client_fio,source_queue_client_update],
    p_source_table_type="queue",
    p_source_id="2"
)

client_rk = DDL.Attribute(
    p_attribute_type="rk",
    p_attribute_id="1",
    p_attribute_name="client_rk",
    p_data_type={"datatype":"int"}
)
client_source = DDL.Attribute(
    p_attribute_type="source",
    p_attribute_id="2",
    p_attribute_name="source_system_id",
    p_data_type={"datatype":"int"}
)

client_etl = DDL.Attribute(
    p_attribute_type="etl",
    p_attribute_id="3",
    p_attribute_name="etl",
    p_data_type={"datatype":"int"}
)

client_fio = DDL.Attribute(
    p_attribute_type="value",
    p_attribute_id="15",
    p_attribute_name="fio",
    p_data_type={"datatype":"varchar","length":10}
)

client_from = DDL.Attribute(
    p_attribute_type="from",
    p_attribute_id="16",
    p_attribute_name="from_dttm",
    p_data_type={"datatype":"timestamp"}
)

client_to = DDL.Attribute(
    p_attribute_type="to",
    p_attribute_id="17",
    p_attribute_name="to_dttm",
    p_data_type={"datatype":"timestamp"}
)

client_table = DDL.Table(
    p_table_name="Client",
    p_table_attrs=[client_rk,client_source,client_etl],
    p_table_type="anchor",
    p_table_id="1",
    p_source_tables=[source_idmap_client_table]
)

client_fio_table = DDL.Table(
    p_table_name="Client_FIO",
    p_table_attrs=[client_rk,client_fio,client_etl, client_from, client_to],
    p_table_type="attribute",
    p_table_id="2",
    p_source_tables=[source_idmap_client_table, source_queue_client_table,source_queue_client_table2]
)



client_etl=DDL.ETL(
    p_table=client_table
)

client_fio_etl=DDL.ETL(
    p_table=client_fio_table
)



print(
    client_fio_etl.get_all_etl_templates()
)

# print(
#     client_etl.get_all_etl_script()
# )
