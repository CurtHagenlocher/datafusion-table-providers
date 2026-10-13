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

//! The end-to-end scenario of the ADBC file-driver proposal (PROTOTYPE):
//!
//! > A DataFusion query joins:
//! >
//! > - a named range `Prices` in `budget.xlsx`, through an Excel file driver
//! >   over the built-in local provider; and
//! > - `orders/*.csv` inside `s3://bucket/archive.zip`, through a CSV file
//! >   driver over a zip provider over an S3 provider.
//! >
//! > Every driver and provider is loaded dynamically through standard
//! > interfaces. DataFusion pushes the column lists and filters down, reads
//! > the partitions in parallel, and performs the join itself.
//!
//! | Piece | Language | Loaded from |
//! |---|---|---|
//! | Excel file driver (`adbc_excel`) | Rust | driver manifest `excel.toml` |
//! | CSV file driver (`adbc_driver_csv`) | C++ | driver manifest `csv.toml` |
//! | local provider (`file:`) | built into the driver manager | — |
//! | zip provider (`adbc_filesystem_zip`) | Rust | provider manifest `zip.toml` |
//! | S3 provider (`adbc_filesystem_object_store`) | Rust | provider manifest `s3.toml` |
//!
//! None of the drivers knows about ZIP or S3, and neither provider knows
//! about the other: the driver manager composes `zip:s3://…!/` from its
//! registry and hands the CSV driver one file system.
//!
//! # Running
//!
//! An S3-compatible endpoint that accepts any credentials, e.g. moto:
//!
//! ```text
//! pip install "moto[server]"
//! moto_server -p 5555
//! ```
//!
//! The libraries, from the arrow-adbc `file-api` branch:
//!
//! ```text
//! cd rust && cargo build -p adbc_excel -p adbc_filesystem_zip -p adbc_filesystem_object_store
//! cmake -S c -B build -DADBC_DRIVER_CSV=ON && cmake --build build --target adbc_driver_csv_shared
//! ```
//!
//! Then point the demo at them and run it:
//!
//! ```text
//! ADBC_EXCEL_DRIVER=…/adbc_excel.dll  ADBC_CSV_DRIVER=…/adbc_driver_csv.dll
//! ADBC_ZIP_PROVIDER=…/adbc_filesystem_zip.dll
//! ADBC_S3_PROVIDER=…/adbc_filesystem_object_store.dll
//! ADBC_S3_ENDPOINT=http://127.0.0.1:5555        (the default)
//! ADBC_DEMO_TRACE=1                              (optional: log S3 requests)
//! cargo run -p datafusion-table-providers-adbc --example file_drivers_demo
//! ```
//!
//! The demo writes the workbook, the archive, and the four manifests into a
//! scratch directory (kept if `ADBC_DEMO_DIR` names one), uploads the
//! archive to the endpoint, and prints the plan and the result.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Driver, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{filesystem, ManagedDatabase, ManagedDriver};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use datafusion_table_providers_adbc::scan_table::AdbcScanTable;
use zip::write::SimpleFileOptions;
use zip::CompressionMethod;

const BUCKET: &str = "adbc-demo";

type Error = Box<dyn std::error::Error>;

fn env_path(name: &str) -> Result<String, Error> {
    let value = std::env::var(name).map_err(|_| format!("set {name} (see the example's docs)"))?;
    Ok(value.replace('\\', "/"))
}

// ---------------------------------------------------------------------------
// The data

const SKUS: [&str; 5] = ["A-1", "B-7", "C-3", "D-4", "Z-9"]; // Z-9 has no price
const REGIONS: [&str; 3] = ["EMEA", "AMER", "APAC"];

/// `budget.xlsx`: a cover sheet, and a price list whose data is the named
/// range `Prices`.
fn write_workbook(path: &Path) -> Result<(), Error> {
    let mut workbook = rust_xlsxwriter::Workbook::new();
    let cover = workbook.add_worksheet();
    cover.set_name("Summary")?;
    cover.write(0, 0, "FY2024 budget")?;

    let catalog = workbook.add_worksheet();
    catalog.set_name("Catalog")?;
    catalog.write(0, 0, "Price list (see the named range Prices)")?;
    for (col, header) in ["sku", "description", "unit_price"].iter().enumerate() {
        catalog.write(2, col as u16 + 1, *header)?;
    }
    let prices = [
        ("A-1", "Anchor bolt", 0.85),
        ("B-7", "Bracket, steel", 12.5),
        ("C-3", "Cable, 10 m", 31.0),
        ("D-4", "Drive unit", 249.0),
    ];
    for (i, (sku, description, price)) in prices.iter().enumerate() {
        let row = i as u32 + 3;
        catalog.write(row, 1, *sku)?;
        catalog.write(row, 2, *description)?;
        catalog.write(row, 3, *price)?;
    }
    workbook.define_name("Prices", "=Catalog!$B$3:$D$7")?;
    workbook.save(path)?;
    Ok(())
}

/// `archive.zip`: a year of monthly order files (and a stray readme),
/// alternately stored and deflated.
fn build_archive() -> Result<Vec<u8>, Error> {
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    zip.start_file("README.txt", SimpleFileOptions::default())?;
    zip.write_all(b"Orders by month.\n")?;
    let mut id = 0u64;
    for month in 1..=12u32 {
        let mut csv = String::from("id,order_date,sku,qty,region\n");
        for day in 0..400u64 {
            let sku = SKUS[(id * 7 % 5) as usize];
            let qty = id * 13 % 9 + 1;
            let region = REGIONS[(id % 3) as usize];
            csv.push_str(&format!(
                "{id},2024-{month:02}-{:02},{sku},{qty},{region}\n",
                day % 28 + 1
            ));
            id += 1;
        }
        let method = if month % 2 == 0 {
            CompressionMethod::Deflated
        } else {
            CompressionMethod::Stored
        };
        zip.start_file(
            format!("orders/2024-{month:02}.csv"),
            SimpleFileOptions::default().compression_method(method),
        )?;
        zip.write_all(csv.as_bytes())?;
    }
    Ok(zip.finish()?.into_inner())
}

/// One HTTP request with a placeholder signature: enough for an emulator
/// such as moto, which does not verify signatures (but treats requests
/// without one as anonymous). Against real S3, upload the archive yourself.
fn http(endpoint: &str, method: &str, path: &str, body: &[u8]) -> Result<String, Error> {
    let host = endpoint.trim_start_matches("http://").trim_end_matches('/');
    let mut stream = std::net::TcpStream::connect(host)?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n\
         Authorization: AWS4-HMAC-SHA256 Credential=demo/20240101/us-east-1/s3/aws4_request, \
         SignedHeaders=host, Signature=0\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

fn upload(endpoint: &str, key: &str, body: &[u8]) -> Result<(), Error> {
    http(endpoint, "PUT", &format!("/{BUCKET}"), b"")?;
    let response = http(endpoint, "PUT", &format!("/{BUCKET}/{key}"), body)?;
    if !response.starts_with("HTTP/1.1 200") {
        return Err(format!("upload to {endpoint} failed: {response}").into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The manifests: how a host finds drivers and providers

fn write_driver_manifest(
    dir: &Path,
    name: &str,
    library: &str,
    entrypoint: &str,
) -> Result<(), Error> {
    std::fs::write(
        dir.join(format!("{name}.toml")),
        format!(
            r#"manifest_version = 1
name = "ADBC {name} file driver (prototype)"

[ADBC]
version = "1.2.0"
min_version = "1.2.0"

[Driver]
shared = "{library}"
entrypoint = "{entrypoint}"
"#
        ),
    )?;
    Ok(())
}

fn write_provider_manifest(
    dir: &Path,
    scheme: &str,
    library: &str,
    layered: bool,
    options: &str,
) -> Result<(), Error> {
    std::fs::write(
        dir.join(format!("{scheme}.toml")),
        format!(
            r#"manifest_version = 1
name = "ADBC {scheme} file-system provider (prototype)"

[ADBC.filesystem]
schemes = ["{scheme}"]
layered = {layered}

[Provider]
shared = "{library}"

[options]
{options}"#
        ),
    )?;
    Ok(())
}

fn load_driver(drivers: &Path, name: &str) -> Result<ManagedDriver, Error> {
    Ok(ManagedDriver::load_from_name(
        name,
        None,
        AdbcVersion::V120,
        LOAD_FLAG_DEFAULT,
        Some(vec![drivers.to_path_buf()]),
    )?)
}

fn open(driver: &mut ManagedDriver, uri: &str) -> Result<Arc<ManagedDatabase>, Error> {
    Ok(Arc::new(driver.new_database_with_opts([(
        OptionDatabase::Uri,
        uri.into(),
    )])?))
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Error> {
    let excel_library = env_path("ADBC_EXCEL_DRIVER")?;
    let csv_library = env_path("ADBC_CSV_DRIVER")?;
    let zip_library = env_path("ADBC_ZIP_PROVIDER")?;
    let s3_library = env_path("ADBC_S3_PROVIDER")?;
    let endpoint =
        std::env::var("ADBC_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:5555".into());
    let trace = std::env::var("ADBC_DEMO_TRACE").is_ok_and(|v| v == "1");

    let scratch;
    let root: PathBuf = match std::env::var("ADBC_DEMO_DIR") {
        Ok(dir) => PathBuf::from(dir),
        Err(_) => {
            scratch = tempfile::TempDir::new()?;
            scratch.path().to_path_buf()
        }
    };
    let (data, drivers, providers) = (
        root.join("data"),
        root.join("adbc/drivers"),
        root.join("adbc/providers"),
    );
    for dir in [&data, &drivers, &providers] {
        std::fs::create_dir_all(dir)?;
    }

    // 1. Data: a workbook on local disk, an archive in object storage.
    let budget = data.join("budget.xlsx");
    write_workbook(&budget)?;
    let archive = build_archive()?;
    upload(&endpoint, "archive.zip", &archive)?;
    println!(
        "data:      {} (local)\n           s3://{BUCKET}/archive.zip ({} bytes, 12 CSV files) at {endpoint}\n",
        budget.display(),
        archive.len()
    );

    // 2. Manifests. A real installation would ship these (e.g. via dbc);
    //    the demo writes them so it is self-contained.
    write_driver_manifest(&drivers, "excel", &excel_library, "AdbcExcelInit")?;
    write_driver_manifest(&drivers, "csv", &csv_library, "AdbcDriverCsvInit")?;
    write_provider_manifest(&providers, "zip", &zip_library, true, "")?;
    write_provider_manifest(
        &providers,
        "s3",
        &s3_library,
        false,
        &format!(
            "aws_endpoint = \"{endpoint}\"\naws_allow_http = \"true\"\naws_region = \"us-east-1\"\n\
             aws_access_key_id = \"demo\"\naws_secret_access_key = \"demo\"\ntrace = \"{trace}\"\n"
        ),
    )?;

    // 3. Load the providers, then the drivers, through their manifests.
    let schemes = filesystem::register_providers_in(&providers)?;
    println!(
        "providers: {} (from {}), plus the built-in file",
        schemes.join(", "),
        providers.display()
    );
    let mut excel = load_driver(&drivers, "excel")?;
    let mut csv = load_driver(&drivers, "csv")?;
    println!("drivers:   excel, csv (from {})\n", drivers.display());

    // 4. One database per source; the driver manager picks each one's file
    //    system from the uri.
    let budget_uri = budget.to_str().unwrap().replace('\\', "/");
    let orders_uri = format!("zip:s3://{BUCKET}/archive.zip!/");
    println!("prices = {budget_uri} :: Prices");
    println!("orders = {orders_uri} :: orders\n");
    let budget_db = open(&mut excel, &budget_uri)?;
    let orders_db = open(&mut csv, &orders_uri)?;

    let ctx = SessionContext::new();
    ctx.register_table(
        "prices",
        Arc::new(AdbcScanTable::try_new(budget_db, "Prices").await?),
    )?;
    ctx.register_table(
        "orders",
        Arc::new(AdbcScanTable::try_new(orders_db, "orders").await?),
    )?;

    // 5. The query.
    let sql = "\
SELECT o.region, p.description,
       SUM(o.qty) AS units,
       ROUND(SUM(o.qty * p.unit_price), 2) AS revenue
FROM orders o
JOIN prices p ON o.sku = p.sku
WHERE o.region <> 'APAC'
  AND o.qty >= 3
  AND o.order_date >= '2024-07-01'
  AND p.unit_price < 100
GROUP BY o.region, p.description
ORDER BY o.region, revenue DESC";
    println!("{sql}\n");

    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    println!(
        "Physical plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    println!(
        "{}",
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)?
    );
    Ok(())
}
