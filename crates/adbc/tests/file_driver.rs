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

//! DataFusion over an ADBC file driver, end to end (PROTOTYPE).
//!
//! Needs the prototype Excel file driver from the arrow-adbc `file-api`
//! branch. Set `ADBC_EXCEL_DRIVER` to the library's path; otherwise the
//! tests look for it in a sibling build and skip if it is not there.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::Driver;
use adbc_driver_manager::{ManagedDatabase, ManagedDriver};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Float64Type, Int64Type};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use datafusion_table_providers_adbc::scan_table::AdbcScanTable;

fn driver_library() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("ADBC_EXCEL_DRIVER") {
        return Some(PathBuf::from(path));
    }
    let name = format!(
        "{}adbc_excel{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let path = Path::new("C:/src/GitHub/arrow-adbc-2/rust/target/debug").join(name);
    path.exists().then_some(path)
}

fn write_workbook(path: &Path) {
    use rust_xlsxwriter::Workbook;
    let mut workbook = Workbook::new();

    let orders = workbook.add_worksheet();
    orders.set_name("Orders").unwrap();
    for (col, name) in ["id", "sku", "qty", "region"].iter().enumerate() {
        orders.write(0, col as u16, *name).unwrap();
    }
    let rows = [
        (1, "A-1", 3, "east"),
        (2, "B-7", 1, "west"),
        (3, "A-1", 12, "west"),
        (4, "C-3", 40, "east"),
        (5, "B-7", 2, "east"),
    ];
    for (i, (id, sku, qty, region)) in rows.iter().enumerate() {
        let r = i as u32 + 1;
        orders.write(r, 0, *id).unwrap();
        orders.write(r, 1, *sku).unwrap();
        orders.write(r, 2, *qty).unwrap();
        orders.write(r, 3, *region).unwrap();
    }

    let catalog = workbook.add_worksheet();
    catalog.set_name("Catalog").unwrap();
    catalog.write(0, 0, "price list").unwrap();
    catalog.write(1, 1, "sku").unwrap();
    catalog.write(1, 2, "unit_price").unwrap();
    for (i, (sku, price)) in [("A-1", 9.0), ("B-7", 115.5), ("C-3", 0.75)]
        .iter()
        .enumerate()
    {
        catalog.write(i as u32 + 2, 1, *sku).unwrap();
        catalog.write(i as u32 + 2, 2, *price).unwrap();
    }
    workbook
        .define_name("Prices", "=Catalog!$B$2:$C$5")
        .unwrap();
    workbook.save(path).unwrap();
}

struct Setup {
    _dir: tempfile::TempDir,
    ctx: SessionContext,
}

async fn setup() -> Option<Setup> {
    let Some(library) = driver_library() else {
        eprintln!("skipping: build adbc_excel or set ADBC_EXCEL_DRIVER");
        return None;
    };
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("budget.xlsx");
    write_workbook(&path);

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        &library,
        Some(b"AdbcExcelInit"),
        AdbcVersion::V120,
    )
    .unwrap();
    assert!(driver.accepts_file_system());
    // The driver manager resolves the uri to its local file system and hands
    // it to the driver; the driver never opens the file itself.
    let database: Arc<ManagedDatabase> = Arc::new(
        driver
            .new_database_with_opts([(OptionDatabase::Uri, path.to_str().unwrap().into())])
            .unwrap(),
    );

    let ctx = SessionContext::new();
    let orders = AdbcScanTable::try_new(Arc::clone(&database), "Orders")
        .await
        .unwrap();
    let prices = AdbcScanTable::try_new(Arc::clone(&database), "Prices")
        .await
        .unwrap();
    ctx.register_table("orders", Arc::new(orders)).unwrap();
    ctx.register_table("prices", Arc::new(prices)).unwrap();
    Some(Setup { _dir: dir, ctx })
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

async fn plan(ctx: &SessionContext, sql: &str) -> String {
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let text = displayable(plan.as_ref()).indent(true).to_string();
    text
}

#[tokio::test]
async fn join_a_sheet_with_a_named_range() {
    // Keep the whole Setup alive: it owns the temporary workbook.
    let Some(setup) = setup().await else {
        return;
    };
    let ctx = &setup.ctx;
    let sql = "SELECT o.sku, SUM(o.qty * p.unit_price) AS total \
               FROM orders o JOIN prices p ON o.sku = p.sku \
               GROUP BY o.sku ORDER BY o.sku";
    let batches = query(ctx, sql).await;
    let batch = datafusion::arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    // DataFusion may hand back Utf8 or Utf8View; compare as Utf8.
    let skus = datafusion::arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
    let skus = skus.as_string::<i32>();
    let skus: Vec<&str> = (0..skus.len()).map(|i| skus.value(i)).collect();
    assert_eq!(skus, ["A-1", "B-7", "C-3"]);
    let totals = batch.column(1).as_primitive::<Float64Type>();
    assert_eq!(totals.value(0), 15.0 * 9.0);
    assert_eq!(totals.value(1), 3.0 * 115.5);
    assert_eq!(totals.value(2), 40.0 * 0.75);

    // Only the columns the query uses were requested from the driver.
    let plan = plan(ctx, sql).await;
    assert!(
        plan.contains(r#"AdbcScanExec: target=Orders, columns=["sku", "qty"]"#),
        "{plan}"
    );
    assert!(
        plan.contains(r#"AdbcScanExec: target=Prices, columns=["sku", "unit_price"]"#),
        "{plan}"
    );
}

#[tokio::test]
async fn filters_are_applied_by_datafusion() {
    // Keep the whole Setup alive: it owns the temporary workbook.
    let Some(setup) = setup().await else {
        return;
    };
    let ctx = &setup.ctx;
    let batches = query(
        ctx,
        "SELECT SUM(qty) AS q FROM orders WHERE region = 'east' AND qty > 2",
    )
    .await;
    let q = batches[0].column(0).as_primitive::<Int64Type>().value(0);
    assert_eq!(q, 3 + 40);
}

#[tokio::test]
async fn count_star_needs_no_columns() {
    // Keep the whole Setup alive: it owns the temporary workbook.
    let Some(setup) = setup().await else {
        return;
    };
    let ctx = &setup.ctx;
    let batches = query(ctx, "SELECT COUNT(*) AS n FROM orders").await;
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 5);
}

#[tokio::test]
async fn limit_is_pushed_down() {
    // Keep the whole Setup alive: it owns the temporary workbook.
    let Some(setup) = setup().await else {
        return;
    };
    let ctx = &setup.ctx;
    let batches = query(ctx, "SELECT id FROM orders LIMIT 2").await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2);
}
