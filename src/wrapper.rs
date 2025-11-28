use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;

use indexmap::IndexMap;
use serde_json::Value as JsonValue;
use sqlx::{Column, Executor, Row};
use sqlx_sqlite_conn_mgr::{SqliteDatabase, SqliteDatabaseConfig};
use tauri::{AppHandle, Manager, Runtime};

use crate::Error;

/// Wrapper around SqliteDatabase that adapts it for the plugin interface
pub struct DatabaseWrapper {
   inner: Arc<SqliteDatabase>,
}

impl DatabaseWrapper {
   /// Connect to a SQLite database via the connection manager
   pub async fn connect<R: Runtime>(
      path: &str,
      app: &AppHandle<R>,
      custom_config: Option<SqliteDatabaseConfig>,
   ) -> Result<Self, Error> {
      // Resolve path relative to app_config_dir
      let abs_path = resolve_database_path(path, app)?;

      // Use connection manager to connect with optional custom config
      let db = SqliteDatabase::connect(&abs_path, custom_config).await?;

      Ok(Self { inner: db })
   }

   /// Execute a write query (INSERT/UPDATE/DELETE)
   pub async fn execute(&self, query: String, values: Vec<JsonValue>) -> Result<(u64, i64), Error> {
      // Prevent read-only queries from triggering WAL mode
      if is_read_only_query(&query) {
         return Err(Error::ReadOnlyQueryInExecute);
      }

      // Acquire writer for mutations
      let mut writer = self.inner.acquire_writer().await?;

      let mut q = sqlx::query(&query);
      for value in values {
         q = bind_value(q, value);
      }

      let result = q.execute(&mut *writer).await?;
      Ok((result.rows_affected(), result.last_insert_rowid()))
   }

   /// Execute a SELECT query, possibly returning multiple rows
   pub async fn fetch_all(
      &self,
      query: String,
      values: Vec<JsonValue>,
   ) -> Result<Vec<IndexMap<String, JsonValue>>, Error> {
      // Use read pool for queries
      let pool = self.inner.read_pool()?;

      let mut q = sqlx::query(&query);
      for value in values {
         q = bind_value(q, value);
      }

      let rows = pool.fetch_all(q).await?;

      // Decode rows to JSON
      let mut values = Vec::new();
      for row in rows {
         let mut value = IndexMap::default();
         for (i, column) in row.columns().iter().enumerate() {
            let v = row.try_get_raw(i)?;
            let v = crate::decode::to_json(v)?;
            value.insert(column.name().to_string(), v);
         }
         values.push(value);
      }

      Ok(values)
   }

   /// Execute a SELECT query expecting zero or one result
   pub async fn fetch_one(
      &self,
      query: String,
      values: Vec<JsonValue>,
   ) -> Result<Option<IndexMap<String, JsonValue>>, Error> {
      // Use read pool for queries
      let pool = self.inner.read_pool()?;

      // Add LIMIT 2 to detect if query returns multiple rows
      // We only need to fetch up to 2 rows to know if there's more than 1
      let limited_query = format!("{} LIMIT 2", query.trim_end_matches(';'));

      let mut q = sqlx::query(&limited_query);
      for value in values {
         q = bind_value(q, value);
      }

      let rows = pool.fetch_all(q).await?;

      // Validate row count
      match rows.len() {
         0 => Ok(None),
         1 => {
            // Decode single row to JSON
            let row = &rows[0];
            let mut value = IndexMap::default();
            for (i, column) in row.columns().iter().enumerate() {
               let v = row.try_get_raw(i)?;
               let v = crate::decode::to_json(v)?;
               value.insert(column.name().to_string(), v);
            }
            Ok(Some(value))
         }
         count => {
            // Multiple rows returned - this is an error
            Err(Error::MultipleRowsReturned(count))
         }
      }
   }

   /// Execute multiple statements atomically within a transaction.
   ///
   /// This method:
   /// 1. Begins a transaction (BEGIN)
   /// 2. Executes all statements in order
   /// 3. Commits on success (COMMIT)
   /// 4. Rolls back on any error (ROLLBACK)
   ///
   /// The writer is held for the entire transaction, ensuring atomicity.
   pub async fn execute_transaction(
      &self,
      statements: Vec<(String, Vec<JsonValue>)>,
   ) -> Result<(), Error> {
      // Acquire writer for the entire transaction
      let mut writer = self.inner.acquire_writer().await?;

      // Begin transaction
      sqlx::query("BEGIN").execute(&mut *writer).await?;

      // Execute all statements, rolling back on error
      let result = async {
         for (query, values) in statements {
            let mut q = sqlx::query(&query);
            for value in values {
               q = bind_value(q, value);
            }
            q.execute(&mut *writer).await?;
         }
         Ok::<(), Error>(())
      }
      .await;

      // Commit or rollback based on result
      match result {
         Ok(()) => {
            sqlx::query("COMMIT").execute(&mut *writer).await?;
            Ok(())
         }
         Err(e) => {
            // Attempt rollback, but preserve original error
            let _ = sqlx::query("ROLLBACK").execute(&mut *writer).await;
            Err(e)
         }
      }
   }

   /// Run migrations on this database
   pub async fn migrate(&self, migrator: &sqlx::migrate::Migrator) -> Result<(), Error> {
      // Migrations need write access
      let mut writer = self.inner.acquire_writer().await?;
      migrator.run(&mut *writer).await?;
      Ok(())
   }

   /// Close the database connection
   pub async fn close(self) -> Result<(), Error> {
      // Close via Arc (handles both owned and shared cases)
      self.inner.close().await?;
      Ok(())
   }

   /// Close the database connection and remove all database files
   pub async fn remove(self) -> Result<(), Error> {
      // Remove via Arc (handles both owned and shared cases)
      self.inner.remove().await?;
      Ok(())
   }
}

/// Helper function to bind a JSON value to a SQLx query
fn bind_value<'a>(
   query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>,
   value: JsonValue,
) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
   if value.is_null() {
      query.bind(None::<JsonValue>)
   } else if value.is_string() {
      query.bind(value.as_str().unwrap().to_owned())
   } else if let Some(number) = value.as_number() {
      query.bind(number.as_f64().unwrap_or_default())
   } else {
      query.bind(value)
   }
}

/// Resolve database file path relative to app config directory
fn resolve_database_path<R: Runtime>(path: &str, app: &AppHandle<R>) -> Result<PathBuf, Error> {
   let app_path = app
      .path()
      .app_config_dir()
      .expect("No App config path was found!");

   create_dir_all(&app_path).expect("Couldn't create app config dir");

   // Join the relative path to the app config directory
   Ok(app_path.join(path))
}

/// Check if a SQL query is read-only (SELECT, EXPLAIN, or read-only PRAGMA)
fn is_read_only_query(query: &str) -> bool {
   // Remove leading whitespace and SQL comments
   let cleaned = strip_sql_comments(query);
   let lowercase = cleaned.to_lowercase();

   // Check for common read-only statement types
   let read_only_keywords = [
      "select",  // SELECT queries
      "explain", // EXPLAIN queries
      "with",    // CTEs (Common Table Expressions)
      "values",  // Standalone VALUES expressions
   ];

   for keyword in &read_only_keywords {
      if lowercase.starts_with(keyword) {
         let keyword_len = keyword.len();

         // Just the keyword alone (e.g., "SELECT" with nothing after)
         if cleaned.len() == keyword_len {
            return true;
         }

         // Ensure it's actually a keyword (followed by space, paren, or semicolon)
         let next_char = cleaned.chars().nth(keyword_len);
         if matches!(
            next_char,
            Some(' ') | Some('\t') | Some('\n') | Some('(') | Some(';')
         ) {
            return true;
         }
      }
   }

   // Special handling for PRAGMA: read-only if no assignment (no '=')
   // Examples:
   //   - Read-only:  "PRAGMA table_info(users)" or "PRAGMA user_version"
   //   - Write:      "PRAGMA journal_mode=WAL" or "PRAGMA foreign_keys=ON"
   if lowercase.starts_with("pragma") {
      let next_char = cleaned.chars().nth("pragma".len());
      if matches!(next_char, Some(' ') | Some('\t') | Some('(')) {
         return !query.contains('=');
      }
   }

   false
}

/// Strip leading SQL comments from a query
fn strip_sql_comments(query: &str) -> String {
   let mut result = query.trim_start();

   loop {
      // Remove line comments (-- comment)
      if result.starts_with("--") {
         if let Some(newline_pos) = result.find('\n') {
            result = result[newline_pos + 1..].trim_start();
         } else {
            // Comment to end of string
            return String::new();
         }
      }
      // Remove block comments (/* comment */)
      else if result.starts_with("/*") {
         if let Some(end_pos) = result.find("*/") {
            result = result[end_pos + 2..].trim_start();
         } else {
            // Unterminated comment - treat as empty
            return String::new();
         }
      } else {
         break;
      }
   }

   result.to_string()
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn test_is_read_only_query_select() {
      assert!(is_read_only_query("SELECT * FROM users"));
      assert!(is_read_only_query("select * from users"));
      assert!(is_read_only_query("  SELECT * FROM users"));
      assert!(is_read_only_query("\nSELECT * FROM users"));
   }

   #[test]
   fn test_is_read_only_query_select_with_parens() {
      assert!(is_read_only_query("SELECT(1)"));
      assert!(is_read_only_query("select (1)"));
   }

   #[test]
   fn test_is_read_only_query_write_operations() {
      assert!(!is_read_only_query("INSERT INTO users VALUES (1)"));
      assert!(!is_read_only_query("UPDATE users SET name = 'test'"));
      assert!(!is_read_only_query("DELETE FROM users"));
      assert!(!is_read_only_query("DROP TABLE users"));
      assert!(!is_read_only_query("CREATE TABLE users (id INTEGER)"));
   }

   #[test]
   fn test_is_read_only_query_explain() {
      assert!(is_read_only_query("EXPLAIN SELECT * FROM users"));
      assert!(is_read_only_query("EXPLAIN QUERY PLAN SELECT * FROM users"));
   }

   #[test]
   fn test_is_read_only_query_with_cte() {
      assert!(is_read_only_query(
         "WITH cte AS (SELECT 1) SELECT * FROM cte"
      ));
      assert!(is_read_only_query(
         "with cte as (select 1) select * from cte"
      ));
   }

   #[test]
   fn test_is_read_only_query_values() {
      assert!(is_read_only_query("VALUES (1, 2, 3)"));
      assert!(is_read_only_query("values(1, 2, 3)"));
   }

   #[test]
   fn test_is_read_only_query_pragma() {
      // Read-only PRAGMAs (no assignment)
      assert!(is_read_only_query("PRAGMA table_info(users)"));
      assert!(is_read_only_query("PRAGMA user_version"));
      assert!(is_read_only_query("PRAGMA database_list"));

      // Write PRAGMAs (with assignment)
      assert!(!is_read_only_query("PRAGMA journal_mode=WAL"));
      assert!(!is_read_only_query("PRAGMA foreign_keys=ON"));
      assert!(!is_read_only_query("PRAGMA user_version = 5"));
   }

   #[test]
   fn test_is_read_only_query_with_comments() {
      assert!(is_read_only_query("-- comment\nSELECT * FROM users"));
      assert!(is_read_only_query("/* comment */SELECT * FROM users"));
      assert!(is_read_only_query("/* multi\nline */SELECT * FROM users"));
   }

   #[test]
   fn test_is_read_only_query_false_positives() {
      // Ensure "select" in string or identifier doesn't trigger
      assert!(!is_read_only_query("INSERT INTO select_table VALUES (1)"));
      assert!(!is_read_only_query("SELECTALL FROM users")); // Not a keyword
   }

   #[test]
   fn test_strip_sql_comments_no_comments() {
      assert_eq!(strip_sql_comments("SELECT 1"), "SELECT 1");
      assert_eq!(strip_sql_comments("  SELECT 1"), "SELECT 1");
   }

   #[test]
   fn test_strip_sql_comments_line_comment() {
      assert_eq!(strip_sql_comments("-- comment\nSELECT 1"), "SELECT 1");
      assert_eq!(strip_sql_comments("--comment\nSELECT 1"), "SELECT 1");
      assert_eq!(strip_sql_comments("-- comment"), "");
   }

   #[test]
   fn test_strip_sql_comments_block_comment() {
      assert_eq!(strip_sql_comments("/* comment */SELECT 1"), "SELECT 1");
      assert_eq!(strip_sql_comments("/*comment*/SELECT 1"), "SELECT 1");
      assert_eq!(strip_sql_comments("/* multi\nline */SELECT 1"), "SELECT 1");
      assert_eq!(strip_sql_comments("/* comment */"), "");
   }

   #[test]
   fn test_strip_sql_comments_multiple() {
      assert_eq!(
         strip_sql_comments("-- line\n/* block */SELECT 1"),
         "SELECT 1"
      );
      assert_eq!(
         strip_sql_comments("/* block */-- line\nSELECT 1"),
         "SELECT 1"
      );
      assert_eq!(strip_sql_comments("-- a\n-- b\nSELECT 1"), "SELECT 1");
   }

   #[test]
   fn test_strip_sql_comments_unterminated() {
      // Unterminated block comment should return empty
      assert_eq!(strip_sql_comments("/* unterminated"), "");
   }

   #[test]
   fn test_strip_sql_comments_preserves_internal() {
      // Only strips leading comments, not internal ones
      let result = strip_sql_comments("SELECT /* internal */ 1");
      assert!(result.starts_with("SELECT"));
   }
}
