use std::sync::Arc;
use std::time::Duration;

use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilder, DataChunk, PrimitiveArrayBuilder, Row};
use risingwave_common::catalog::{SchemaId, TableId};
use risingwave_common::types::DecimalType;
use risingwave_common::util::addr::get_host_port;
use risingwave_common::util::downcast_arc;
use risingwave_pb::common::HostAddress;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::expr_node::Type::InputRef;
use risingwave_pb::expr::{ExprNode, InputRefExpr};
use risingwave_pb::plan::column_desc::ColumnEncodingType;
use risingwave_pb::plan::{
    ColumnDesc, ColumnOrder, DatabaseRefId, OrderType, SchemaRefId, TableRefId,
};
use risingwave_pb::stream_plan::dispatcher::DispatcherType;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::table_source_node::SourceType;
use risingwave_pb::stream_plan::{
    Dispatcher, MViewNode, ProjectNode, StreamFragment, StreamNode, TableSourceNode,
};
use risingwave_pb::stream_service::{ActorInfo, BroadcastActorInfoTableRequest};
use risingwave_source::{MemSourceManager, SourceManager};
use risingwave_storage::bummock::BummockTable;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::{SimpleTableManager, TableManager};
use risingwave_storage::{Table, TableColumnDesc};

use crate::executor::MViewTable;
use crate::task::{StreamManager, StreamTaskEnv};

fn make_int32_type_pb() -> DataType {
    DataType {
        type_name: TypeName::Int32 as i32,
        ..Default::default()
    }
}

fn make_table_ref_id(id: i32) -> TableRefId {
    TableRefId {
        schema_ref_id: Some(SchemaRefId {
            database_ref_id: Some(DatabaseRefId { database_id: 0 }),
            schema_id: 0,
        }),
        table_id: id,
    }
}

#[tokio::test]
async fn test_stream_mv_proto() {
    let port = 2333;
    // Build example proto for a stream executor chain.
    // TableSource -> Project -> Materialized View
    // Select v1 from T(v1,v2).
    let source_proto = StreamNode {
        node: Some(Node::TableSourceNode(TableSourceNode {
            table_ref_id: Some(make_table_ref_id(0)),
            column_ids: vec![0, 1],
            source_type: SourceType::Table as i32,
        })),
        input: vec![],
        pk_indices: vec![],
    };
    let expr_proto = ExprNode {
        expr_type: InputRef as i32,
        return_type: Some(make_int32_type_pb()),
        rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: 0 })),
    };
    let column_desc = ColumnDesc {
        column_type: Some(DataType {
            type_name: TypeName::Int32 as i32,
            ..Default::default()
        }),
        encoding: ColumnEncodingType::Raw as i32,
        is_primary: false,
        name: "v1".to_string(),
        column_id: 0,
    };

    let project_proto = StreamNode {
        node: Some(Node::ProjectNode(ProjectNode {
            select_list: vec![expr_proto],
        })),
        input: vec![source_proto],
        pk_indices: vec![],
    };
    let mview_proto = StreamNode {
        node: Some(Node::MviewNode(MViewNode {
            table_ref_id: Some(make_table_ref_id(1)),
            column_descs: vec![column_desc],
            pk_indices: vec![0],
            column_orders: vec![ColumnOrder {
                order_type: OrderType::Ascending as i32,
                input_ref: Some(InputRefExpr { column_idx: 0 }),
                return_type: Some(DataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
            }],
        })),
        input: vec![project_proto],
        pk_indices: vec![],
    };
    let fragment_proto = StreamFragment {
        actor_id: 1,
        nodes: Some(mview_proto),
        dispatcher: Some(Dispatcher {
            r#type: DispatcherType::Simple as i32,
            column_idx: 0,
        }),
        downstream_actor_id: vec![233],
    };

    // Initialize storage.
    let table_manager = Arc::new(SimpleTableManager::new());
    let source_manager = Arc::new(MemSourceManager::new());
    let table_id = TableId::default();
    let table_columns = vec![
        TableColumnDesc {
            column_id: 0,
            data_type: Arc::new(DecimalType::new(false, 10, 5).unwrap()),
        },
        TableColumnDesc {
            column_id: 1,
            data_type: Arc::new(DecimalType::new(false, 10, 5).unwrap()),
        },
    ];
    let table = table_manager
        .create_table(&table_id, table_columns)
        .await
        .unwrap();
    source_manager
        .create_table_source(
            &table_id,
            downcast_arc::<BummockTable>(table.into_any()).unwrap(),
        )
        .unwrap();

    let table_ref =
        downcast_arc::<BummockTable>(table_manager.get_table(&table_id).unwrap().into_any())
            .unwrap();
    // Mock initial data.
    // One row of (1,2)
    let mut array_builder1 = PrimitiveArrayBuilder::<i32>::new(1).unwrap();
    array_builder1.append(Some(1_i32)).unwrap();
    let array1 = array_builder1.finish().unwrap();
    let column1 = Column::new(Arc::new(array1.into()));
    let mut array_builder2 = PrimitiveArrayBuilder::<i32>::new(1).unwrap();
    array_builder2.append(Some(2_i32)).unwrap();
    let array2 = array_builder2.finish().unwrap();
    let column2 = Column::new(Arc::new(array2.into()));
    let columns = vec![column1, column2];
    let append_chunk = DataChunk::builder().columns(columns).build();

    // Build stream actor.
    let socket_addr = get_host_port(&format!("127.0.0.1:{}", port)).unwrap();
    let stream_manager = StreamManager::new(socket_addr, None);
    let env = StreamTaskEnv::new(table_manager.clone(), source_manager, socket_addr);

    let actor_info_proto = ActorInfo {
        actor_id: 1,
        host: Some(HostAddress {
            host: "127.0.0.1".into(),
            port,
        }),
    };
    let actor_info_proto2 = ActorInfo {
        actor_id: 233,
        host: Some(HostAddress {
            host: "127.0.0.1".into(),
            port,
        }),
    };
    let actor_info_table = BroadcastActorInfoTableRequest {
        info: vec![actor_info_proto, actor_info_proto2],
    };
    stream_manager.update_actor_info(actor_info_table).unwrap();
    stream_manager.update_fragment(&[fragment_proto]).unwrap();
    stream_manager.build_fragment(&[1], env).unwrap();

    // Insert data and check if the materialized view has been updated.
    let _res_app = table_ref.append(append_chunk).await;
    let table_id_mv = TableId::new(SchemaId::default(), 1);
    let table = downcast_arc::<MViewTable<MemoryStateStore>>(
        table_manager.get_table(&table_id_mv).unwrap().into_any(),
    )
    .unwrap();

    // FIXME: We have to make sure that `append_chunk` has been processed by the actor first,
    // then we can send the stop barrier.
    tokio::time::sleep(Duration::from_millis(500)).await;
    stream_manager.send_stop_barrier();
    // TODO(MrCroxx): fix this
    // FIXME: use channel when testing hummock to make sure local state has already flushed.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let datum = table
        .get(Row(vec![Some(1_i32.into())]), 0)
        .await
        .unwrap()
        .unwrap();

    let d_value = datum.unwrap().into_int32();
    assert_eq!(d_value, 1);
}
