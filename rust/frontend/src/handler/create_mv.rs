use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{ObjectName, Query, Statement};

use crate::binder::Binder;
use crate::planner::Planner;
use crate::session::QueryContext;

pub async fn handle_create_materialized_view(
    context: QueryContext<'_>,
    table_name: ObjectName,
    query: Box<Query>,
) -> Result<PgResponse> {
    let session = context.session;

    let catalog_mgr = session.env().catalog_mgr();
    let catalog = catalog_mgr
        .get_database_snapshot(session.database())
        .unwrap();
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(Statement::Query(query))?;
    let mut planner = Planner::new();
    let logical = planner.plan(bound)?;
    let batch = logical.gen_create_mv_plan();

    Ok(PgResponse::new(
        StatementType::CREATE_MATERIALIZED_VIEW,
        0,
        vec![],
        vec![],
    ))
}
