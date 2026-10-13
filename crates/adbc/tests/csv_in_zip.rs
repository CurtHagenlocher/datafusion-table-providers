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

//! DataFusion over several CSV files in one ZIP archive, read in parallel
//! (PROTOTYPE). The CSV file driver is C++, the ZIP provider and the driver
//! manager are Rust; none of them knows about the others.
//!
//! Set `ADBC_CSV_DRIVER` to the prototype CSV driver library; otherwise the
//! tests are skipped.

use std::io::Write;
use std::sync::{Arc, Once};

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::Driver;
use adbc_driver_manager::{filesystem, ManagedDatabase, ManagedDriver};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use datafusion_table_providers_adbc::scan_table::AdbcScanTable;
use zip::write::SimpleFileOptions;
use zip::CompressionMethod;

fn month(first_id: usize, rows: usize) -> String {
    let mut out = String::from("id,sku,qty\n");
    for i in 0..rows {
        let id = first_id + i;
        out.push_str(&format!("{id},S-{},{}\n", id % 3, id % 5 + 1));
    }
    out
}

struct Setup {
    _dir: tempfile::TempDir,
    ctx: SessionContext,
}

async fn setup() -> Option<Setup> {
    let library = std::env::var("ADBC_CSV_DRIVER").ok()?;
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        filesystem::register_scheme(
            "zip",
            Arc::new(adbc_filesystem_zip::factory(filesystem::resolve)),
        );
    });

    let dir = tempfile::TempDir::new().unwrap();
    let archive = dir.path().join("orders.zip");
    let mut zip = zip::ZipWriter::new(std::fs::File::create(&archive).unwrap());
    for (i, (first, rows)) in [(0, 1000), (1000, 2500), (3500, 500), (4000, 1000)]
        .iter()
        .enumerate()
    {
        let method = if i % 2 == 0 {
            CompressionMethod::Stored
        } else {
            CompressionMethod::Deflated
        };
        zip.start_file(
            format!("orders/2024-0{}.csv", i + 1),
            SimpleFileOptions::default().compression_method(method),
        )
        .unwrap();
        zip.write_all(month(*first, *rows).as_bytes()).unwrap();
    }
    zip.finish().unwrap();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        library,
        Some(b"AdbcDriverCsvInit"),
        AdbcVersion::V120,
    )
    .unwrap();
    let uri = format!("zip:{}!/", archive.to_str().unwrap().replace('\\', "/"));
    let database: Arc<ManagedDatabase> = Arc::new(
        driver
            .new_database_with_opts([(OptionDatabase::Uri, uri.as_str().into())])
            .unwrap(),
    );
    let ctx = SessionContext::new();
    let orders = AdbcScanTable::try_new(database, "orders").await.unwrap();
    ctx.register_table("orders", Arc::new(orders)).unwrap();
    Some(Setup { _dir: dir, ctx })
}

#[tokio::test]
async fn aggregates_over_parallel_partitions() {
    let Some(setup) = setup().await else {
        eprintln!("skipping: set ADBC_CSV_DRIVER");
        return;
    };
    let ctx = &setup.ctx;
    let sql = "SELECT sku, COUNT(*) AS n, SUM(qty) AS qty FROM orders GROUP BY sku ORDER BY sku";

    // Four files in the archive: four ADBC partitions, four DataFusion
    // partitions, read concurrently; only the needed columns are requested.
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains(r#"AdbcScanExec: target=orders, columns=["sku", "qty"], partitions=4"#),
        "{plan_text}"
    );

    let batches: Vec<RecordBatch> = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let batch = datafusion::arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let skus = datafusion::arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
    let skus = skus.as_string::<i32>();
    assert_eq!(
        (0..skus.len()).map(|i| skus.value(i)).collect::<Vec<_>>(),
        ["S-0", "S-1", "S-2"]
    );
    let counts = batch.column(1).as_primitive::<Int64Type>();
    let total: i64 = (0..counts.len()).map(|i| counts.value(i)).sum();
    assert_eq!(total, 5000);
    let qty = batch.column(2).as_primitive::<Int64Type>();
    let expected_qty: i64 = (0..5000i64).map(|id| id % 5 + 1).sum();
    assert_eq!(
        (0..qty.len()).map(|i| qty.value(i)).sum::<i64>(),
        expected_qty
    );
}

#[tokio::test]
async fn count_and_filter() {
    let Some(setup) = setup().await else {
        return;
    };
    let ctx = &setup.ctx;
    let batches: Vec<RecordBatch> = ctx
        .sql("SELECT COUNT(*) FROM orders WHERE id >= 3500 AND id < 4000")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        500
    );
}

async fn plan_of(ctx: &SessionContext, sql: &str) -> String {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let text = displayable(plan.as_ref()).indent(true).to_string();
    text
}

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches: Vec<RecordBatch> = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

#[tokio::test]
async fn filters_are_pushed_to_the_driver() {
    let Some(setup) = setup().await else {
        return;
    };
    let ctx = &setup.ctx;

    // Both conjuncts are exact: the driver filters, DataFusion does not.
    let sql = "SELECT COUNT(*) FROM orders WHERE id >= 3500 AND id < 4000";
    let plan = plan_of(ctx, sql).await;
    assert!(plan.contains("filters=["), "{plan}");
    assert!(!plan.contains("FilterExec"), "{plan}");
    assert_eq!(count(ctx, sql).await, 500);

    // Three-valued logic and OR, still exact.
    let sql = "SELECT COUNT(*) FROM orders WHERE NOT (qty > 2 OR sku = 'S-1')";
    assert!(!plan_of(ctx, sql).await.contains("FilterExec"));
    let expected = (0..5000i64)
        .filter(|id| !(id % 5 + 1 > 2 || id % 3 == 1))
        .count() as i64;
    assert_eq!(count(ctx, sql).await, expected);

    // LIKE is not something the driver evaluates: it stays in DataFusion,
    // while the comparison is still pushed down.
    let sql = "SELECT COUNT(*) FROM orders WHERE qty > 2 AND sku LIKE '%-2'";
    let plan = plan_of(ctx, sql).await;
    assert!(plan.contains("FilterExec"), "{plan}");
    assert!(plan.contains("filters=[qty > Int64(2)]"), "{plan}");
    let expected = (0..5000i64)
        .filter(|id| id % 5 + 1 > 2 && id % 3 == 2)
        .count() as i64;
    assert_eq!(count(ctx, sql).await, expected);
}
