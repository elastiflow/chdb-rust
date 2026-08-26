/// Example: Parameterized Queries
///
/// A small inventory lookup that binds request filters to ClickHouse
/// `{name:Type}` placeholders with `execute_with_params`, instead of
/// splicing values into the SQL string.
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use chdb_rust::query_param::QueryParams;
use chdb_rust::session::SessionBuilder;

fn main() -> Result<(), chdb_rust::error::Error> {
    println!("=== Inventory Lookup (Parameterized Queries) ===\n");

    let tmp_dir = std::env::temp_dir().join("chdb-inventory-params");
    let session = SessionBuilder::new()
        .with_data_path(tmp_dir)
        .with_auto_cleanup(true)
        .build()?;

    println!("1. Creating warehouse stock table...");

    session.execute(
        "CREATE DATABASE warehouse; USE warehouse",
        Some(&[Arg::MultiQuery]),
    )?;

    session.execute(
        "CREATE TABLE stock (
            sku String,
            product String,
            category String,
            warehouse String,
            qty UInt32,
            unit_cost Float64
        ) ENGINE = MergeTree() ORDER BY (warehouse, sku)",
        None,
    )?;

    println!("2. Loading sample stock...");

    session.execute(
        "INSERT INTO stock VALUES
        ('SKU-100', 'USB-C cable', 'cables', 'east', 42, 4.50),
        ('SKU-101', 'HDMI cable', 'cables', 'east', 8, 7.25),
        ('SKU-200', 'Laptop stand', 'desks', 'east', 15, 29.00),
        ('SKU-201', 'Monitor arm', 'desks', 'west', 3, 89.00),
        ('SKU-300', 'Webcam', 'peripherals', 'west', 22, 54.00),
        ('SKU-301', 'USB hub', 'peripherals', 'east', 5, 18.75),
        ('SKU-302', 'Docking station', 'peripherals', 'west', 1, 149.00)",
        None,
    )?;

    // Filters that would normally come from an HTTP request or CLI args.
    let warehouse = "east";
    let category = "cables";

    println!("3. '{category}' stock in warehouse '{warehouse}':\n");

    // Same-typed values can be passed as an array of (name, value) tuples;
    // `Into<QueryParam>` converts each value for the C API.
    let result = session.execute_with_params(
        "SELECT sku, product, qty, unit_cost
         FROM stock
         WHERE warehouse = {warehouse:String}
           AND category = {category:String}
         ORDER BY sku ASC",
        Some(&[Arg::OutputFormat(OutputFormat::Pretty)]),
        [("warehouse", warehouse), ("category", category)],
    )?;

    println!("{}", result.data_utf8_lossy());
    println!();

    let max_qty = 10_u32;
    let min_cost = 50.0_f64;

    println!("4. Items with qty <= {max_qty} or unit_cost >= ${min_cost:.2}:\n");

    // Mixed types go through `QueryParams`, which still uses `Into<QueryParam>`
    // on each `bind` call.
    let result = session.execute_with_params(
        "SELECT warehouse, sku, product, qty, unit_cost
         FROM stock
         WHERE qty <= {max_qty:UInt32}
            OR unit_cost >= {min_cost:Float64}
         ORDER BY warehouse ASC, sku ASC",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
        QueryParams::new()
            .bind("max_qty", max_qty)
            .bind("min_cost", min_cost),
    )?;

    println!("{}", result.data_utf8_lossy());

    Ok(())
}
