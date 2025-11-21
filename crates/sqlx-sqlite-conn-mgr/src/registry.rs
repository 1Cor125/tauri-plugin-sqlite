//! Global database registry to cache new database instances and return existing ones

use crate::Result;
use crate::database::SqliteDatabase;
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use tokio::sync::RwLock;

/// Global registry for SQLite databases
static DATABASE_REGISTRY: OnceLock<RwLock<HashMap<PathBuf, Weak<SqliteDatabase>>>> =
   OnceLock::new();

fn registry() -> &'static RwLock<HashMap<PathBuf, Weak<SqliteDatabase>>> {
   DATABASE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Check if a path represents an in-memory SQLite database
///
/// Returns true for `:memory:` and `file::memory:*` URIs
pub fn is_memory_database(path: &Path) -> bool {
   let path_str = path.to_str().unwrap_or("");
   path_str == ":memory:"
      || path_str.starts_with("file::memory:")
      || path_str.contains("mode=memory")
}

/// Get or open a SQLite database connection
///
/// If a database is already connected, returns the cached instance.
/// Otherwise, calls the provided factory function to create a new connection.
///
/// Special case: `:memory:` databases should not be cached (each is unique)
pub async fn get_or_open_database<F, Fut>(path: &Path, factory: F) -> Result<Arc<SqliteDatabase>>
where
   F: FnOnce() -> Fut,
   Fut: Future<Output = Result<SqliteDatabase>>,
{
   // Skip registry for in-memory databases - always create new
   if is_memory_database(path) {
      let db = factory().await?;
      return Ok(Arc::new(db));
   }

   // Canonicalize the path for consistent lookups
   let canonical_path = canonicalize_path(path)?;

   // Try to get existing database with read lock (allows concurrent reads)
   {
      let registry = registry().read().await;

      if let Some(weak) = registry.get(&canonical_path) {
         if let Some(db) = weak.upgrade() {
            return Ok(db);
         }
         // Weak reference exists but dead - will be cleaned up in write phase
      }
   }

   // Phase 2: Database not found, acquire write lock
   let mut registry = registry().write().await;

   // Double-check: another thread might have created it while we waited for write lock
   if let Some(weak) = registry.get(&canonical_path) {
      if let Some(db) = weak.upgrade() {
         return Ok(db);
      }
   }

   // Clean up dead weak references while we have the write lock
   registry.retain(|_, weak| weak.strong_count() > 0);

   // Now we're sure the database doesn't exist - create it while holding the lock
   // This prevents race conditions
   let db = factory().await?;
   let arc_db = Arc::new(db);

   // Cache the new database
   registry.insert(canonical_path, Arc::downgrade(&arc_db));

   Ok(arc_db)
}

/// Helper to canonicalize a database path
///
/// This function attempts to resolve paths to their canonical form to ensure
/// consistent cache lookups. It handles:
/// - Absolute path resolution
/// - Symlink resolution (when file exists)
/// - Parent directory canonicalization (when file doesn't exist yet)
///
/// Known limitations when file doesn't exist:
/// - Case sensitivity: On case-insensitive filesystems (macOS, Windows), paths
///   differing only in case will be treated as different until the file is created.
///   This could lead to multiple connection pools for the same logical database, at
///   least until the file is created and can be canonicalized properly.
/// - Symlinks in filename: If the filename itself will be a symlink (rare for SQLite),
///   different symlink names won't be resolved until the file exists.
fn canonicalize_path(path: &Path) -> std::io::Result<PathBuf> {
   match path.canonicalize() {
      Ok(p) => Ok(p),
      Err(_) => {
         // If path doesn't exist, try to canonicalize parent + filename
         let parent = path.parent().unwrap_or_else(|| Path::new("."));
         let filename = path
            .file_name()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid path"))?;

         let canonical_parent = parent.canonicalize()?;

         // Note: We preserve the filename case as provided. On case-insensitive
         // filesystems, this means "MyDB.db" and "mydb.db" will create separate
         // cache entries until the file exists and can be canonicalized properly.
         // This is a known limitation but acceptable since:
         // 1. Most apps use consistent casing
         // 2. After first connection creates the file, subsequent connects will
         //    use the canonical (on-disk) case
         Ok(canonical_parent.join(filename))
      }
   }
}

/// Remove a database from the cache
///
/// Special case: `:memory:` databases are never in the registry
///
/// Returns an error if the path cannot be canonicalized
pub async fn uncache_database(path: &Path) -> std::io::Result<()> {
   // Skip registry for in-memory databases
   if is_memory_database(path) {
      return Ok(());
   }

   // Canonicalize path
   let canonical_path = canonicalize_path(path)?;

   let mut registry = registry().write().await;
   registry.remove(&canonical_path);
   Ok(())
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn test_canonicalize_path() {
      let temp_dir = std::env::temp_dir();
      let test_path = temp_dir.join("test.db");

      // Test that path is canonicalized to absolute path
      let canonical = canonicalize_path(&test_path).unwrap();
      assert!(canonical.is_absolute());

      // Test relative path
      let relative_path = Path::new("./test_relative.db");
      let canonical_relative = canonicalize_path(relative_path).unwrap();
      assert!(canonical_relative.is_absolute());
   }

   #[test]
   fn test_canonicalize_nonexistent_path() {
      let temp_dir = std::env::temp_dir();
      let nonexistent = temp_dir.join("nonexistent_dir").join("test.db");

      // Should fail if parent directory doesn't exist
      let result = canonicalize_path(&nonexistent);
      assert!(result.is_err());
   }
}
