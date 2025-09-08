use std::collections::{BTreeSet, HashMap};
use std::net::ToSocketAddrs;

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use futures_util::TryStreamExt;
use rayon::prelude::*;
use serde::Serialize;
use tiberius::{AuthMethod, Client, Config, QueryItem};
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tower_http::{limit::RequestBodyLimitLayer, services::ServeDir};

// =============== App State & Types ===============

#[derive(Clone)]
struct AppConfig {
    sql_host: String,
    sql_port: u16,
}

#[derive(Serialize)]
struct DatabasesResponse {
    databases: Vec<String>,
}

// Precomputed reference entry for faster matching
#[derive(Clone)]
struct RefEntry {
    code: String,
    desc_raw: String,
    desc_norm: String,
    tokens: BTreeSet<String>,
}

// =============== Main ===============

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let sql_host = std::env::var("SQLSERVER_HOST").unwrap_or_else(|_| "mjm-sql01".to_string());
    let sql_port: u16 = std::env::var("SQLSERVER_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1433);

    let app_state = AppConfig { sql_host, sql_port };

    let app = Router::new()
        .route("/api/databases", get(list_databases))
        .route("/api/upload", post(upload_and_check))
        .nest_service("/", ServeDir::new("public").append_index_html_on_directories(true))
        .with_state(app_state)
        // Cap uploads to e.g. 15MB, and disable the default body limit so we only have one source of truth
        .layer(RequestBodyLimitLayer::new(15 * 1024 * 1024))
        .layer(DefaultBodyLimit::disable());

    let listener_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    let listener = tokio::net::TcpListener::bind(&listener_addr)
        .await
        .expect("failed to bind address");
    println!("Server running at http://{}", listener_addr);
    axum::serve(listener, app).await.expect("server error");
}

// =============== Routes ===============

async fn list_databases(
    State(cfg): State<AppConfig>,
) -> Result<Json<DatabasesResponse>, (StatusCode, String)> {
    match fetch_databases(&cfg.sql_host, cfg.sql_port).await {
        Ok(dbs) => Ok(Json(DatabasesResponse { databases: dbs })),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

async fn upload_and_check(
    State(cfg): State<AppConfig>,
    mut multipart: Multipart,
) -> Result<Response, (StatusCode, String)> {
    // Parse fields: db (text) and file (binary) and optional threshold
    let mut selected_db: Option<String> = None;
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut similarity_threshold: f64 = 0.3; // default 30%

    while let Some(field) = multipart.next_field().await.map_err(internal_error)? {
        let name = field.name().map(|s| s.to_string());
        match name.as_deref() {
            Some("db") => {
                let val = field.text().await.map_err(client_error)?; // 400 on bad text read
                selected_db = Some(val);
            }
            Some("file") => {
                // Optional content-type guard, if desired:
                // if let Some(ct) = field.content_type() {
                //     if !ct.contains("spreadsheetml") && !ct.contains("excel") {
                //         return Err((StatusCode::BAD_REQUEST, format!("unsupported content-type: {}", ct)));
                //     }
                // }
                let data = field.bytes().await.map_err(client_error)?;
                file_bytes = Some(data.to_vec());
            }
            Some("threshold") => {
                let raw = field.text().await.map_err(client_error)?;
                let parsed = raw
                    .parse::<f64>()
                    .map_err(|_| (StatusCode::BAD_REQUEST, "invalid threshold".to_string()))?;
                similarity_threshold = parsed.clamp(0.0, 1.0);
            }
            _ => {
                // ignore unknown fields
            }
        }
    }

    let db_name = selected_db.ok_or_else(|| (StatusCode::BAD_REQUEST, "missing db".to_string()))?;
    let file_data =
        file_bytes.ok_or_else(|| (StatusCode::BAD_REQUEST, "missing file".to_string()))?;

    // Read input Excel (xlsx) – expect two columns: Item Code, Item Description
    use calamine::{DataType, Reader, Xlsx};
    use std::io::Cursor;
    let cursor = Cursor::new(file_data);
    let mut workbook = Xlsx::new(cursor).map_err(client_error)?;

    let range = workbook
        .worksheet_range_at(0)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "No first sheet".to_string()))?
        .map_err(client_error)?;

    // Detect headers (case/space-insensitive)
    let mut code_col = 0usize;
    let mut desc_col = 1usize;
    if let Some(header_row) = range.rows().next() {
        let find_idx = |needle: &str| -> Option<usize> {
            let needle_norm = header_key(needle);
            header_row.iter().position(|cell| {
                let s = cell.get_string().unwrap_or("");
                header_key(s) == needle_norm
            })
        };
        if let Some(i) = find_idx("Item Code") {
            code_col = i;
        }
        if let Some(i) = find_idx("Item Description") {
            desc_col = i;
        }
    } else {
        return Err((StatusCode::BAD_REQUEST, "Sheet is empty".to_string()));
    }

    // Read rows from Excel (skip header)
    let mut excel_rows: Vec<(String, String)> = Vec::new();
    for row in range.rows().skip(1) {
        let code = row
            .get(code_col)
            .and_then(DataType::get_string)
            .unwrap_or("")
            .trim()
            .to_string();
        let desc = row
            .get(desc_col)
            .and_then(DataType::get_string)
            .unwrap_or("")
            .trim()
            .to_string();
        if !code.is_empty() || !desc.is_empty() {
            excel_rows.push((code, desc));
        }
    }

    // Fetch reference data from DB: Item_descriptions.ADB_Ref, Item_Description
    let refs = fetch_item_descriptions(&cfg.sql_host, cfg.sql_port, &db_name)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    // Build lookup map and precomputed reference entries
    let mut adb_ref_to_descs: HashMap<String, Vec<String>> = HashMap::new();
    let mut all_pairs: Vec<(String, String)> = Vec::new();
    for (adb_ref, desc) in refs.iter() {
        adb_ref_to_descs
            .entry(adb_ref.clone())
            .or_default()
            .push(desc.clone());
        all_pairs.push((adb_ref.clone(), desc.clone()));
    }
    let ref_entries = build_ref_entries(&all_pairs);

    // Prepare output workbook
    use rust_xlsxwriter::{Format, FormatAlign, Workbook};
    let mut wb = Workbook::new();

    // Header/column formats
    let hdr = Format::new().set_bold().set_align(FormatAlign::Center);
    let hdr_wrap = Format::new()
        .set_bold()
        .set_align(FormatAlign::Center)
        .set_text_wrap();
    let wrap = Format::new().set_text_wrap();
    let percent_wrap = Format::new().set_text_wrap().set_num_format("0.00%");

    // First sheet: Matches
    let mut match_sheet = wb.add_worksheet();
    match_sheet.set_name("Match").map_err(server_error)?;
    write_row_strings(
        &mut match_sheet,
        0,
        &[
            "Item Code",
            "Item Description",
            "Matched ADB_Ref",
            "Matched DB Description",
        ],
        Some(&hdr_wrap),
    )
    .map_err(server_error)?;
    let mut match_row_idx = 1u32;

    // Formatting: First tab (Match)
    // - Set column B and D to width 40
    match_sheet.set_column_width(1, 40.0).map_err(server_error)?;
    match_sheet.set_column_width(3, 40.0).map_err(server_error)?;
    // - Wrap text for all cells in existing columns (A..D)
    for col in 0..=3u16 {
        match_sheet
            .set_column_format(col, &wrap)
            .map_err(server_error)?;
    }

    // Collect mismatches and no-code rows
    let mut mismatch_rows: Vec<(String, String, String, Vec<(String, String, f64)>)> = Vec::new();
    let mut no_match_rows: Vec<(String, String, String, Vec<(String, String, f64)>)> = Vec::new();

    for (code, desc) in excel_rows.into_iter() {
        let code_trim = code.trim();
        if let Some(db_descs) = adb_ref_to_descs.get(code_trim) {
            let input_norm = normalize_description(&desc);
            if let Some(matched_db_desc) = db_descs
                .iter()
                .find(|d| normalize_description(d) == input_norm)
            {
                // Code exists and normalized description matches
                write_row_strings(
                    &mut match_sheet,
                    match_row_idx,
                    &[code_trim, &desc, code_trim, matched_db_desc],
                    None,
                )
                .map_err(server_error)?;
                match_row_idx += 1;
            } else {
                // Code exists but description differs — suggest
                let sample_db_desc = db_descs.get(0).cloned().unwrap_or_default();
                let suggestions = find_best_matches(&desc, &ref_entries, similarity_threshold);
                mismatch_rows.push((
                    code_trim.to_string(),
                    desc.clone(),
                    sample_db_desc,
                    suggestions,
                ));
            }
        } else {
            // No code match — suggest
            let suggestions = find_best_matches(&desc, &ref_entries, similarity_threshold);
            no_match_rows.push((
                code_trim.to_string(),
                desc.clone(),
                "No ADB_Ref match".to_string(),
                suggestions,
            ));
        }
    }
    drop(match_sheet); // release borrow before creating other sheets

    // Second sheet: Description mismatches
    let mut mismatch_sheet = wb.add_worksheet();
    mismatch_sheet
        .set_name("Description_Mismatch")
        .map_err(server_error)?;
    write_row_strings(
        &mut mismatch_sheet,
        0,
        &[
            "Item Code",
            "Input Description",
            "DB Description (sample)",
            "Suggested Code",
            "Suggested Description",
            "Similarity",
        ],
        Some(&hdr_wrap),
    )
    .map_err(server_error)?;
    let mut mismatch_row_idx = 1u32;
    // Formatting: Second tab (Description_Mismatch)
    // - Set column B, C, E to width 40
    mismatch_sheet.set_column_width(1, 40.0).map_err(server_error)?;
    mismatch_sheet.set_column_width(2, 40.0).map_err(server_error)?;
    mismatch_sheet.set_column_width(4, 40.0).map_err(server_error)?;
    // - Wrap text for all cells (A..F)
    for col in 0..=5u16 {
        mismatch_sheet
            .set_column_format(col, &wrap)
            .map_err(server_error)?;
    }
    // - Column F as percentage (and wrapped)
    mismatch_sheet
        .set_column_format(5, &percent_wrap)
        .map_err(server_error)?;
    for (code, input_desc, db_desc, suggestions) in mismatch_rows.into_iter() {
        if suggestions.is_empty() {
            write_row_mismatch(
                &mut mismatch_sheet,
                mismatch_row_idx,
                &code,
                &input_desc,
                &db_desc,
                None,
            )
            .map_err(server_error)?;
            mismatch_row_idx += 1;
        } else {
            for (s_code, s_desc, score) in suggestions.iter() {
                write_row_mismatch(
                    &mut mismatch_sheet,
                    mismatch_row_idx,
                    &code,
                    &input_desc,
                    &db_desc,
                    Some((s_code, s_desc, *score)),
                )
                .map_err(server_error)?;
                mismatch_row_idx += 1;
            }
        }
    }
    drop(mismatch_sheet);

    // Third sheet: No code matches
    let mut no_match_sheet = wb.add_worksheet();
    no_match_sheet
        .set_name("No_Code_Match")
        .map_err(server_error)?;
    write_row_strings(
        &mut no_match_sheet,
        0,
        &[
            "Item Code",
            "Item Description",
            "Reason",
            "Suggested Code",
            "Suggested Description",
            "Similarity",
        ],
        Some(&hdr_wrap),
    )
    .map_err(server_error)?;
    let mut no_match_row_idx = 1u32;
    // Formatting: Third tab (No_Code_Match)
    // - Set column B, C, E to width 40
    no_match_sheet.set_column_width(1, 40.0).map_err(server_error)?;
    no_match_sheet.set_column_width(2, 40.0).map_err(server_error)?;
    no_match_sheet.set_column_width(4, 40.0).map_err(server_error)?;
    // - Wrap text for all cells (A..F)
    for col in 0..=5u16 {
        no_match_sheet
            .set_column_format(col, &wrap)
            .map_err(server_error)?;
    }
    // - Column F as percentage (and wrapped)
    no_match_sheet
        .set_column_format(5, &percent_wrap)
        .map_err(server_error)?;
    for (code, desc, reason, suggestions) in no_match_rows.into_iter() {
        if suggestions.is_empty() {
            write_row_no_code(&mut no_match_sheet, no_match_row_idx, &code, &desc, &reason, None)
                .map_err(server_error)?;
            no_match_row_idx += 1;
        } else {
            for (s_code, s_desc, score) in suggestions.iter() {
                write_row_no_code(
                    &mut no_match_sheet,
                    no_match_row_idx,
                    &code,
                    &desc,
                    &reason,
                    Some((s_code, s_desc, *score)),
                )
                .map_err(server_error)?;
                no_match_row_idx += 1;
            }
        }
    }

    let bytes: Vec<u8> = wb.save_to_buffer().map_err(server_error)?;

    // Build response
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
    );

    // Timestamped filename w/ db
    let now = chrono::Local::now();
    let fname = format!(
        "results_{}_{}.xlsx",
        sanitize_filename(&db_name),
        now.format("%Y%m%d_%H%M%S")
    );
    headers.insert(
        axum::http::header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{}\"", fname))
            .unwrap_or(HeaderValue::from_static("attachment; filename=results.xlsx")),
    );

    let resp = Response::builder()
        .status(StatusCode::OK)
        .header(
            axum::http::header::CONTENT_TYPE,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        .header(
            axum::http::header::CONTENT_DISPOSITION,
            headers[axum::http::header::CONTENT_DISPOSITION].clone(),
        )
        .body(Body::from(bytes))
        .map_err(server_error)?;

    Ok(resp)
}

// =============== DB Helpers ===============

async fn fetch_databases(host: &str, port: u16) -> Result<Vec<String>, String> {
    // Build Tiberius config
    let mut config = Config::new();
    config.host(host);
    config.trust_cert(); // NOTE: consider real TLS in production
    config.authentication(AuthMethod::Integrated);
    config.port(port);

    // Proactively resolve the address
    let _ = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("address resolve error: {}", e))?;

    // Connect TCP and upgrade to compat for tiberius
    let tcp = tokio::net::TcpStream::connect((host, port))
        .await
        .map_err(|e| format!("TCP connect error: {}", e))?;
    let tcp = tcp.compat_write();

    let mut client = Client::connect(config, tcp)
        .await
        .map_err(|e| format!("DB connect error: {}", e))?;

    // List user databases (exclude system DBs)
    let query = "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb')";
    let mut stream = client
        .query(query, &[])
        .await
        .map_err(|e| format!("DB list error: {}", e))?;

    let mut dbs = Vec::new();
    while let Some(item) = stream
        .try_next()
        .await
        .map_err(|e| format!("DB list row error: {}", e))?
    {
        if let QueryItem::Row(row) = item {
            let name: &str = row
                .get(0)
                .ok_or_else(|| "missing name column".to_string())
                .map_err(|e| e.to_string())?;
            dbs.push(name.to_string());
        }
    }

    Ok(dbs)
}

async fn fetch_item_descriptions(
    host: &str,
    port: u16,
    database: &str,
) -> Result<Vec<(String, String)>, String> {
    let mut config = Config::new();
    config.host(host);
    config.trust_cert(); // NOTE: consider real TLS in production
    config.authentication(AuthMethod::Integrated);
    config.port(port);
    config.database(database);

    let tcp = tokio::net::TcpStream::connect((host, port))
        .await
        .map_err(|e| format!("TCP connect error: {}", e))?;
    let tcp = tcp.compat_write();
    let mut client =
        Client::connect(config, tcp)
            .await
            .map_err(|e| format!("DB connect error: {}", e))?;

    let query = "SELECT ADB_Ref, Item_Description FROM Item_descriptions";
    let mut stream = client
        .query(query, &[])
        .await
        .map_err(|e| format!("query error: {}", e))?;
    let mut rows = Vec::new();
    while let Some(item) = stream
        .try_next()
        .await
        .map_err(|e| format!("row error: {}", e))?
    {
        if let QueryItem::Row(row) = item {
            let code: &str = row.get(0).unwrap_or("");
            let desc: &str = row.get(1).unwrap_or("");
            rows.push((code.to_string(), desc.to_string()));
        }
    }
    Ok(rows)
}

// =============== Matching & Normalization ===============

fn normalize_description(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join(" ")
}

fn tokenize(s: &str) -> BTreeSet<String> {
    s.split_whitespace().map(|w| w.to_string()).collect()
}

fn build_ref_entries(all_refs: &[(String, String)]) -> Vec<RefEntry> {
    all_refs
        .iter()
        .map(|(code, desc)| {
            let desc_norm = normalize_description(desc);
            RefEntry {
                code: code.clone(),
                desc_raw: desc.clone(),
                tokens: tokenize(&desc_norm),
                desc_norm,
            }
        })
        .collect()
}

/// Fuzzy matching using weighted Jaccard (0.7) + Levenshtein (0.3).
fn find_best_matches(input_desc: &str, refs: &[RefEntry], threshold: f64) -> Vec<(String, String, f64)> {
    let input_norm = normalize_description(input_desc);
    let input_tokens = tokenize(&input_norm);

    let mut out: Vec<(String, String, f64)> = refs
        .par_iter()
        .map(|r| {
            let j = jaccard(&input_tokens, &r.tokens);
            let max_len = input_norm.len().max(r.desc_norm.len());
            let lev = if max_len == 0 {
                1.0
            } else {
                1.0 - (strsim::levenshtein(&input_norm, &r.desc_norm) as f64 / max_len as f64)
            };
            let score = (j * 0.7) + (lev * 0.3);
            (r.code.clone(), r.desc_raw.clone(), score)
        })
        .filter(|(_, _, s)| *s >= threshold)
        .collect();

    out.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
    out
}

fn jaccard(a: &BTreeSet<String>, b: &BTreeSet<String>) -> f64 {
    let intersection = a.intersection(b).count();
    let union = a.len() + b.len() - intersection;
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

// =============== XLSX Helpers ===============

fn write_row_strings(
    sheet: &mut rust_xlsxwriter::Worksheet,
    row: u32,
    cols: &[&str],
    fmt: Option<&rust_xlsxwriter::Format>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for (i, v) in cols.iter().enumerate() {
        if let Some(f) = fmt {
            sheet.write_string_with_format(row, i as u16, *v, f)?;
        } else {
            sheet.write_string(row, i as u16, *v)?;
        }
    }
    Ok(())
}

fn write_row_mismatch(
    sheet: &mut rust_xlsxwriter::Worksheet,
    row: u32,
    code: &str,
    input_desc: &str,
    db_desc: &str,
    suggestion: Option<(&str, &str, f64)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sheet.write_string(row, 0, code)?;
    sheet.write_string(row, 1, input_desc)?;
    sheet.write_string(row, 2, db_desc)?;
    match suggestion {
        Some((s_code, s_desc, score)) => {
            sheet.write_string(row, 3, s_code)?;
            sheet.write_string(row, 4, s_desc)?;
            sheet.write_number(row, 5, score)?;
        }
        None => {
            sheet.write_string(row, 3, "No suggestions")?;
            sheet.write_string(row, 4, "No suggestions")?;
            sheet.write_number(row, 5, 0.0)?;
        }
    }
    Ok(())
}

fn write_row_no_code(
    sheet: &mut rust_xlsxwriter::Worksheet,
    row: u32,
    code: &str,
    desc: &str,
    reason: &str,
    suggestion: Option<(&str, &str, f64)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sheet.write_string(row, 0, code)?;
    sheet.write_string(row, 1, desc)?;
    sheet.write_string(row, 2, reason)?;
    match suggestion {
        Some((s_code, s_desc, score)) => {
            sheet.write_string(row, 3, s_code)?;
            sheet.write_string(row, 4, s_desc)?;
            sheet.write_number(row, 5, score)?;
        }
        None => {
            sheet.write_string(row, 3, "No suggestions")?;
            sheet.write_string(row, 4, "No suggestions")?;
            sheet.write_number(row, 5, 0.0)?;
        }
    }
    Ok(())
}

// =============== Misc Utils & Errors ===============

fn header_key(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_lowercase()
}

fn sanitize_filename(s: &str) -> String {
    // Very basic: replace anything non-alnum/underscore/dash with underscore
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn internal_error<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

fn server_error<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

fn client_error<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}
