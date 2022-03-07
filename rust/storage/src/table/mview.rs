use std::borrow::Cow;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::Datum;
use risingwave_common::util::get_value_columns;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;

use super::TableIterRef;
use crate::cell_based_row_deserializer::CellBasedRowDeserializer;
use crate::table::{ScannableTable, TableIter};
use crate::{dispatch_state_store, Keyspace, StateStore, StateStoreImpl, TableColumnDesc};

pub fn new_adhoc_mview_table(
    state_store: StateStoreImpl,
    table_id: &TableId,
    column_ids: &[ColumnId],
    fields: &[Field],
    pk_column_indices: &[usize],
    order_types: &[OrderType],
) -> Arc<dyn ScannableTable> {
    dispatch_state_store!(state_store, state_store, {
        let keyspace = Keyspace::table_root(state_store, table_id);
        let table = MViewTable::new_adhoc(
            keyspace,
            pk_column_indices.to_vec(),
            order_types.to_vec(),
            column_ids,
            fields,
        );
        Arc::new(table) as Arc<dyn ScannableTable>
    })
}

/// `MViewTable` provides a readable cell-based row table interface,
/// so that data can be queried by AP engine.
pub struct MViewTable<S: StateStore> {
    keyspace: Keyspace<S>,

    schema: Schema,

    column_descs: Vec<TableColumnDesc>,

    order_types: Vec<OrderType>,

    pk_column_indices: Vec<usize>,

    value_column_indices: Vec<usize>,

    sort_key_serializer: OrderedRowSerializer,
}

impl<S: StateStore> std::fmt::Debug for MViewTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MViewTable")
            .field("schema", &self.schema)
            .field("pk_column_indices", &self.pk_column_indices)
            .field("value_column_indices", &self.value_column_indices)
            .finish()
    }
}

impl<S: StateStore> MViewTable<S> {
    /// Create a [`MViewTable`] for materialized view.
    // TODO(bugen): remove this...
    pub fn new_for_test(
        keyspace: Keyspace<S>,
        schema: Schema,
        pk_columns: Vec<usize>,
        order_types: Vec<OrderType>,
    ) -> Self {
        assert_eq!(pk_columns.len(), order_types.len());
        let column_descs = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_index, f)| {
                // For mview, column id is exactly the index, so we perform conversion here.
                let column_id = ColumnId::from(column_index as i32);
                TableColumnDesc::unnamed(column_id, f.data_type.clone())
            })
            .collect_vec();

        let value_columns = get_value_columns(&pk_columns, &schema.data_types());

        Self {
            keyspace,
            schema,
            column_descs,
            order_types: order_types.clone(),
            pk_column_indices: pk_columns,
            value_column_indices: value_columns,
            sort_key_serializer: OrderedRowSerializer::new(order_types),
        }
    }

    /// Create an "adhoc" [`MViewTable`] with specified columns.
    // TODO: remove this and refactor into `RowTable`.
    pub fn new_adhoc(
        keyspace: Keyspace<S>,
        pk_column_indices: Vec<usize>,
        order_types: Vec<OrderType>,
        column_ids: &[ColumnId],
        fields: &[Field],
    ) -> Self {
        assert_eq!(pk_column_indices.len(), order_types.len());
        let schema = Schema::new(fields.to_vec());
        let column_descs = column_ids
            .iter()
            .zip_eq(fields.iter())
            .map(|(column_id, field)| TableColumnDesc::unnamed(*column_id, field.data_type.clone()))
            .collect();
        let value_column_indices = get_value_columns(&pk_column_indices, &schema.data_types());

        Self {
            keyspace,
            schema,
            column_descs,
            order_types: order_types.clone(),
            pk_column_indices,
            value_column_indices,
            sort_key_serializer: OrderedRowSerializer::new(order_types),
        }
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    async fn iter(&self, epoch: u64) -> Result<MViewTableIter<S>> {
        let pk_data_types = self
            .pk_column_indices
            .iter()
            .map(|idx| self.schema.data_types()[*idx].clone())
            .collect::<Vec<_>>();

        println!(
            "iter pk_data_types:{:?},order_types:{:?}",
            pk_data_types, self.order_types
        );

        let ordered_row_deserializer =
            OrderedRowDeserializer::new(pk_data_types, self.order_types.clone());
        MViewTableIter::new(
            self.keyspace.clone(),
            self.column_descs.clone(),
            self.value_column_indices.clone(),
            ordered_row_deserializer,
            epoch,
        )
        .await
    }

    // TODO(MrCroxx): More interfaces are needed besides cell get.
    // The returned Datum is from a snapshot corresponding to the given `epoch`
    // TODO(eric): remove this...
    // TODO(bugen): remove this...
    pub async fn get_for_test(
        &self,
        pk: Row,
        cell_idx: usize,
        epoch: u64,
    ) -> Result<Option<Datum>> {
        assert!(
            !self.pk_column_indices.is_empty(),
            "this table is adhoc and there's no pk information"
        );

        debug_assert!(cell_idx < self.schema.len());
        // TODO(MrCroxx): More efficient encoding is needed.

        let buf = self
            .keyspace
            .get(
                &[
                    &serialize_pk(&pk, &self.sort_key_serializer)?[..],
                    &serialize_column_id(&ColumnId::from(cell_idx as i32))?
                    // &serialize_cell_idx(cell_idx as i32)?[..],
                ]
                .concat(),
                epoch,
            )
            .await
            .map_err(|err| ErrorCode::InternalError(err.to_string()))?;

        if let Some(buf) = buf {
            Ok(Some(deserialize_cell(
                &buf[..],
                &self.schema.fields[cell_idx].data_type,
            )?))
        } else {
            Ok(None)
        }
    }
}

pub struct MViewTableIter<S: StateStore> {
    keyspace: Keyspace<S>,

    /// A buffer to store prefetched kv pairs from state store
    buf: Vec<(Bytes, Bytes)>,
    /// The idx into `buf` for the next item
    next_idx: usize,
    /// A bool to indicate whether there are more data to fetch from state store
    done: bool,
    /// Cached error messages after the iteration completes or fails
    err_msg: Option<String>,
    /// A epoch representing the read snapshot
    epoch: u64,
    /// Cell-based row deserializer
    cell_based_row_deserializer: CellBasedRowDeserializer,
    /// Ordered row deserializer
    ordered_row_deserializer: OrderedRowDeserializer,
    /// Value column indices
    value_columns: Vec<usize>,
}

impl<'a, S: StateStore> MViewTableIter<S> {
    // TODO: adjustable limit
    const SCAN_LIMIT: usize = 1024;

    async fn new(
        keyspace: Keyspace<S>,
        table_descs: Vec<TableColumnDesc>,
        value_columns: Vec<usize>,
        ordered_row_deserializer: OrderedRowDeserializer,
        epoch: u64,
    ) -> Result<Self> {
        keyspace.state_store().wait_epoch(epoch).await;

        let value_descs = value_columns
            .iter()
            .map(|idx| table_descs[*idx].clone())
            .collect::<Vec<_>>();

        let cell_based_row_deserializer = CellBasedRowDeserializer::new(value_descs);

        let iter = Self {
            keyspace,
            buf: vec![],
            next_idx: 0,
            done: false,
            err_msg: None,
            epoch,
            cell_based_row_deserializer,
            ordered_row_deserializer,
            value_columns,
        };
        Ok(iter)
    }

    async fn consume_more(&mut self) -> Result<()> {
        assert_eq!(self.next_idx, self.buf.len());

        if self.buf.is_empty() {
            self.buf = self
                .keyspace
                .scan_strip_prefix(Some(Self::SCAN_LIMIT), self.epoch)
                .await?;
        } else {
            let last_key = self.buf.last().unwrap().0.clone();
            let buf = self
                .keyspace
                .scan_with_start_key(last_key.to_vec(), Some(Self::SCAN_LIMIT), self.epoch)
                .await?;
            assert!(!buf.is_empty());
            assert_eq!(buf.first().as_ref().unwrap().0, last_key);
            // TODO: remove the unnecessary clone here
            self.buf = buf[1..].to_vec();
        }

        self.next_idx = 0;

        Ok(())
    }

    fn compose_pk_and_value_as_one_row(&self, mut pk_row: Row, mut value_row: Row) -> Row {
        println!("pk_row:{:?},value_row:{:?}", pk_row, value_row);
        let mut pk_row_idx = 0;
        let mut value_row_idx = 0;
        let mut complete_row_data = vec![];
        while value_row_idx < value_row.0.len() || pk_row_idx < pk_row.0.len() {
            if value_row_idx < value_row.0.len()
                && complete_row_data.len() == self.value_columns[value_row_idx]
            {
                complete_row_data.push(std::mem::take(value_row.0.get_mut(value_row_idx).unwrap()));
                value_row_idx += 1;
            } else {
                complete_row_data.push(std::mem::take(pk_row.0.get_mut(pk_row_idx).unwrap()));
                pk_row_idx += 1;
            }
        }
        Row(complete_row_data)
    }
}

#[async_trait::async_trait]
impl<S: StateStore> TableIter for MViewTableIter<S> {
    async fn next(&mut self) -> Result<Option<Row>> {
        if self.done {
            match &self.err_msg {
                Some(e) => return Err(ErrorCode::InternalError(e.clone()).into()),
                None => return Ok(None),
            }
        }

        loop {
            let (key, value) = match self.buf.get(self.next_idx) {
                Some(kv) => kv,
                None => {
                    // Need to consume more from state store
                    self.consume_more().await?;
                    if let Some(item) = self.buf.first() {
                        item
                    } else {
                        self.done = true;
                        let pk_and_row = self.cell_based_row_deserializer.take();
                        return match pk_and_row {
                            Some(_) => {
                                let (pk, value_row) = pk_and_row.unwrap();
                                println!("pk:{:?},value_row:{:?}", pk, value_row);
                                assert_eq!(value_row.0.len(), self.value_columns.len());
                                let pk_row =
                                    self.ordered_row_deserializer.deserialize(&pk)?.into_row();
                                Ok(Some(
                                    self.compose_pk_and_value_as_one_row(pk_row, value_row),
                                ))
                            }
                            None => Ok(None),
                        };
                    }
                }
            };
            tracing::trace!(
                target: "events::stream::mview::scan",
                "mview scanned key = {:?}, value = {:?}",
                bytes::Bytes::copy_from_slice(key),
                bytes::Bytes::copy_from_slice(value)
            );

            let pk_and_row = self.cell_based_row_deserializer.deserialize(key, value)?;
            self.next_idx += 1;
            match pk_and_row {
                Some(_) => {
                    let (pk, value_row) = pk_and_row.unwrap();
                    println!("pk:{:?},value_row:{:?}", pk, value_row);
                    assert_eq!(value_row.0.len(), self.value_columns.len());
                    let pk_row = self.ordered_row_deserializer.deserialize(&pk)?.into_row();
                    return Ok(Some(
                        self.compose_pk_and_value_as_one_row(pk_row, value_row),
                    ));
                }
                None => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl<S> ScannableTable for MViewTable<S>
where
    S: StateStore,
{
    async fn iter(&self, epoch: u64) -> Result<TableIterRef> {
        Ok(Box::new(self.iter(epoch).await?))
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send> {
        self
    }

    fn schema(&self) -> Cow<Schema> {
        Cow::Borrowed(&self.schema)
    }

    fn column_descs(&self) -> Cow<[TableColumnDesc]> {
        Cow::Borrowed(&self.column_descs)
    }

    fn is_shared_storage(&self) -> bool {
        true
    }
}
