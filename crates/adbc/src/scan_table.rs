// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A table provider over an ADBC *file driver*: a driver that reads one
//! file format through a host-provided file system and exposes scans, not
//! SQL.
//!
//! PROTOTYPE for the ADBC file-driver proposal. Unlike
//! [AdbcDBTable](crate::sql_table::AdbcDBTable), which unparses the plan to
//! SQL, this provider describes each scan with statement options and reads
//! it through `ExecutePartitions` / `ReadPartition`, one DataFusion
//! partition per ADBC partition. Projection and limit are pushed down;
//! filters are left to DataFusion.
//!
//! The statement options follow the prototype convention of the file-driver
//! proposal (`adbc.file.*`); they are not yet part of the ADBC spec.

use std::sync::Arc;

use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Optionable, Statement};
use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

/// Statement option: the object to scan (a sheet, dataset, file, ...).
pub const OPTION_TARGET: &str = "adbc.file.target";
/// Statement option: the columns to return, as a JSON array of names.
pub const OPTION_COLUMNS: &str = "adbc.file.columns";
/// Statement option: the maximum number of rows to return.
pub const OPTION_LIMIT: &str = "adbc.file.limit";

fn adbc_error(error: adbc_core::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Run blocking ADBC calls off the async executor.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> adbc_core::error::Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .map_err(adbc_error)
}

/// One object of a file driver, as a DataFusion table.
///
/// `D` is any ADBC database: one loaded through the driver manager, or a
/// Rust driver used directly.
pub struct AdbcScanTable<D> {
    database: Arc<D>,
    target: String,
    schema: SchemaRef,
}

impl<D> std::fmt::Debug for AdbcScanTable<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AdbcScanTable")
            .field("target", &self.target)
            .field("schema", &self.schema)
            .finish()
    }
}

impl<D> AdbcScanTable<D>
where
    D: Database + Send + Sync + 'static,
{
    /// A table for `target`, with its schema from `GetTableSchema`.
    pub async fn try_new(database: Arc<D>, target: impl Into<String>) -> Result<Self> {
        let target = target.into();
        let schema = {
            let database = Arc::clone(&database);
            let target = target.clone();
            blocking(move || {
                let connection = database.new_connection()?;
                connection.get_table_schema(None, None, &target)
            })
            .await?
        };
        Ok(Self {
            database,
            target,
            schema: Arc::new(schema),
        })
    }
}

#[async_trait]
impl<D> TableProvider for AdbcScanTable<D>
where
    D: Database + Send + Sync + 'static,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Filter pushdown (Substrait ExtendedExpression, with exact/inexact
        // reporting) is a later step of the proposal.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let indices: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.schema.fields().len()).collect(),
        };
        let projected = Arc::new(self.schema.project(&indices)?);
        // An empty projection (count(*)) still needs row counts: ask for
        // one column and drop it when reading.
        let requested: Vec<String> = if indices.is_empty() {
            self.schema
                .fields()
                .first()
                .map(|f| vec![f.name().clone()])
                .unwrap_or_default()
        } else {
            projected
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        };

        let partitions = {
            let database = Arc::clone(&self.database);
            let target = self.target.clone();
            let columns = encode_json_strings(&requested);
            blocking(move || {
                let mut connection = database.new_connection()?;
                let mut statement = connection.new_statement()?;
                statement.set_option(
                    OptionStatement::Other(OPTION_TARGET.into()),
                    target.as_str().into(),
                )?;
                statement.set_option(
                    OptionStatement::Other(OPTION_COLUMNS.into()),
                    columns.as_str().into(),
                )?;
                if let Some(limit) = limit {
                    statement.set_option(
                        OptionStatement::Other(OPTION_LIMIT.into()),
                        OptionValue::Int(limit as i64),
                    )?;
                }
                Ok(statement.execute_partitions()?.partitions)
            })
            .await?
        };

        Ok(Arc::new(AdbcScanExec::new(
            Arc::clone(&self.database),
            self.target.clone(),
            requested,
            partitions,
            projected,
            indices.is_empty(),
        )))
    }
}

/// Reads the partitions of one scan, one DataFusion partition each.
pub struct AdbcScanExec<D> {
    database: Arc<D>,
    target: String,
    columns: Vec<String>,
    partitions: Arc<Vec<Vec<u8>>>,
    schema: SchemaRef,
    /// The scan feeds count(*): return rows without columns.
    rows_only: bool,
    properties: Arc<PlanProperties>,
}

impl<D> AdbcScanExec<D> {
    fn new(
        database: Arc<D>,
        target: String,
        columns: Vec<String>,
        partitions: Vec<Vec<u8>>,
        schema: SchemaRef,
        rows_only: bool,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            database,
            target,
            columns,
            partitions: Arc::new(partitions),
            schema,
            rows_only,
            properties,
        }
    }

    /// The columns requested from the driver (for tests and EXPLAIN).
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

impl<D> std::fmt::Debug for AdbcScanExec<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "AdbcScanExec target={} columns={:?} partitions={}",
            self.target,
            self.columns,
            self.partitions.len()
        )
    }
}

impl<D> DisplayAs for AdbcScanExec<D> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "AdbcScanExec: target={}, columns={:?}, partitions={}",
            self.target,
            self.columns,
            self.partitions.len()
        )
    }
}

impl<D> ExecutionPlan for AdbcScanExec<D>
where
    D: Database + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "AdbcScanExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(2);
        let Some(descriptor) = self.partitions.get(partition).cloned() else {
            // No ADBC partitions: one empty DataFusion partition.
            drop(tx);
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            )));
        };
        let database = Arc::clone(&self.database);
        let rows_only = self.rows_only;
        let batch_schema = Arc::clone(&schema);
        tokio::task::spawn_blocking(move || {
            let read = || -> Result<()> {
                let connection = database.new_connection().map_err(adbc_error)?;
                let reader = connection.read_partition(&descriptor).map_err(adbc_error)?;
                for batch in reader {
                    let batch = batch?;
                    let batch = if rows_only {
                        RecordBatch::try_new_with_options(
                            Arc::clone(&batch_schema),
                            vec![],
                            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                        )?
                    } else {
                        RecordBatch::try_new(Arc::clone(&batch_schema), batch.columns().to_vec())?
                    };
                    if tx.blocking_send(Ok(batch)).is_err() {
                        // The consumer went away.
                        return Ok(());
                    }
                }
                Ok(())
            };
            if let Err(e) = read() {
                let _ = tx.blocking_send(Err(e));
            }
        });
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Encode names as a JSON array of strings.
fn encode_json_strings(items: &[String]) -> String {
    let mut out = String::from("[");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        for c in item.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\t' => out.push_str("\\t"),
                '\r' => out.push_str("\\r"),
                c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                c => out.push(c),
            }
        }
        out.push('"');
    }
    out.push(']');
    out
}
