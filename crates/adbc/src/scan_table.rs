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
//! partition per ADBC partition. Projection, limit, and filters are pushed
//! down. Filters travel as one Substrait `ExtendedExpression` with one
//! expression per conjunct; the driver says which conjuncts it applies
//! exactly, and DataFusion re-applies the rest.
//!
//! The statement options follow the prototype convention of the file-driver
//! proposal (`adbc.file.*`); they are not yet part of the ADBC spec.

use std::sync::Arc;

use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Optionable, Statement};
use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_substrait::logical_plan::producer::to_substrait_extended_expr;
use prost::Message;

/// Statement option: the object to scan (a sheet, dataset, file, ...).
pub const OPTION_TARGET: &str = "adbc.file.target";
/// Statement option: the columns to return, as a JSON array of names.
pub const OPTION_COLUMNS: &str = "adbc.file.columns";
/// Statement option: the maximum number of rows to return.
pub const OPTION_LIMIT: &str = "adbc.file.limit";
/// Statement option (bytes): a Substrait `ExtendedExpression` whose
/// expressions are the conjuncts of a filter.
pub const OPTION_FILTER: &str = "adbc.file.filter";
/// Statement option (read back after setting the filter): per conjunct,
/// `"exact"`, `"inexact"`, or `"unsupported"`, as a JSON array.
pub const OPTION_FILTER_PUSHDOWN: &str = "adbc.file.filter.pushdown";

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
    /// For encoding filters as Substrait.
    state: SessionState,
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
            state: SessionStateBuilder::new().build(),
        })
    }

    /// Encode filters as one `ExtendedExpression`, one expression each,
    /// over the table's schema.
    fn encode_filters(&self, filters: &[&Expr]) -> Result<Vec<u8>> {
        let schema = Arc::new(DFSchema::try_from(self.schema.as_ref().clone())?);
        // The engine may qualify columns with the table's alias; the base
        // schema is unqualified.
        let filters = filters
            .iter()
            .map(|f| {
                (*f).clone()
                    .transform(|e| {
                        Ok(match e {
                            Expr::Column(c) if c.relation.is_some() => {
                                Transformed::yes(Expr::Column(Column::new_unqualified(c.name)))
                            }
                            e => Transformed::no(e),
                        })
                    })
                    .map(|t| t.data)
            })
            .collect::<Result<Vec<_>>>()?;
        let field = Field::new("filter", DataType::Boolean, true);
        let pairs: Vec<(&Expr, &Field)> = filters.iter().map(|f| (f, &field)).collect();
        let encoded = to_substrait_extended_expr(&pairs, &schema, &self.state)?;
        Ok(encoded.encode_to_vec())
    }

    /// Ask the driver which conjuncts it applies. Support depends on the
    /// expressions only, so this does no I/O beyond opening a connection.
    fn ask_pushdown(&self, filter: Vec<u8>) -> adbc_core::error::Result<Vec<String>> {
        let mut connection = self.database.new_connection()?;
        let mut statement = connection.new_statement()?;
        statement.set_option(
            OptionStatement::Other(OPTION_TARGET.into()),
            self.target.as_str().into(),
        )?;
        statement.set_option(
            OptionStatement::Other(OPTION_FILTER.into()),
            OptionValue::Bytes(filter),
        )?;
        let answer =
            statement.get_option_string(OptionStatement::Other(OPTION_FILTER_PUSHDOWN.into()))?;
        Ok(decode_json_strings(&answer))
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
        let mut answer = vec![TableProviderFilterPushDown::Unsupported; filters.len()];
        // Only conjuncts that Substrait can express are offered.
        let offered: Vec<usize> = (0..filters.len())
            .filter(|&i| self.encode_filters(&filters[i..=i]).is_ok())
            .collect();
        if offered.is_empty() {
            return Ok(answer);
        }
        let encoded =
            self.encode_filters(&offered.iter().map(|&i| filters[i]).collect::<Vec<_>>())?;
        // A driver that does not take filters (an error setting the
        // option) supports none of them.
        let Ok(support) = self.ask_pushdown(encoded) else {
            return Ok(answer);
        };
        for (&i, s) in offered.iter().zip(support) {
            answer[i] = match s.as_str() {
                "exact" => TableProviderFilterPushDown::Exact,
                "inexact" => TableProviderFilterPushDown::Inexact,
                _ => TableProviderFilterPushDown::Unsupported,
            };
        }
        Ok(answer)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
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

        // Only the filters the driver accepted reach here.
        let filter = if filters.is_empty() {
            None
        } else {
            Some(self.encode_filters(&filters.iter().collect::<Vec<_>>())?)
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
                if let Some(filter) = filter {
                    statement.set_option(
                        OptionStatement::Other(OPTION_FILTER.into()),
                        OptionValue::Bytes(filter),
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
            filters.iter().map(|f| f.to_string()).collect(),
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
    /// The filters the driver applies, for EXPLAIN.
    filters: Vec<String>,
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
        filters: Vec<String>,
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
            filters,
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

    /// The filters the driver applies (for tests and EXPLAIN).
    pub fn filters(&self) -> &[String] {
        &self.filters
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
            "AdbcScanExec: target={}, columns={:?}, ",
            self.target, self.columns,
        )?;
        if !self.filters.is_empty() {
            write!(f, "filters=[{}], ", self.filters.join(", "))?;
        }
        write!(f, "partitions={}", self.partitions.len())
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

/// Decode a JSON array of plain strings (the pushdown answer).
fn decode_json_strings(text: &str) -> Vec<String> {
    text.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect()
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
