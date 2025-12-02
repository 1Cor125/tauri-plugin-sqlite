use sqlx_sqlite_conn_mgr::{Error, SqliteDatabase, SqliteDatabaseConfig};
use std::sync::Arc;

#[tokio::test]
async fn test_concurrent_reads() {
   use std::sync::atomic::{AtomicUsize, Ordering};
   use tokio::sync::Barrier;

   let db = SqliteDatabase::connect("for_readonly_tests.db", None)
      .await
      .unwrap();
   let barrier = Arc::new(Barrier::new(3));
   let (active, max_seen) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));

   let handles: Vec<_> = (0..3)
      .map(|_| {
         let (db, barrier, active, max_seen) = (
            Arc::clone(&db),
            Arc::clone(&barrier),
            Arc::clone(&active),
            Arc::clone(&max_seen),
         );

         tokio::spawn(async move {
            barrier.wait().await;
            max_seen.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM numbers")
               .fetch_one(db.read_pool().unwrap())
               .await
               .unwrap();
            assert_eq!(count, 12);

            active.fetch_sub(1, Ordering::SeqCst);
         })
      })
      .collect();

   for handle in handles {
      handle.await.unwrap();
   }

   assert_eq!(
      max_seen.load(Ordering::SeqCst),
      3,
      "Expected 3 concurrent reads, but only {} were active simultaneously",
      max_seen.load(Ordering::SeqCst)
   );
}

#[tokio::test]
async fn test_database_closed_error() {
   use std::fs;

   // Create a test database (file will be created if it doesn't exist)
   let test_path = std::env::current_dir().unwrap().join("test_close_error.db");
   let db = SqliteDatabase::connect(&test_path, None)
      .await
      .expect("Failed to connect to test database");

   // Clone db so we can use it after close
   let db_ref = Arc::clone(&db);
   db.close().await.unwrap();

   // Try to use read_pool after close - should error
   let read_result = db_ref.read_pool();
   assert!(read_result.is_err());
   assert!(matches!(read_result.unwrap_err(), Error::DatabaseClosed));

   // Try to acquire writer after close - should error
   let writer_result = db_ref.acquire_writer().await;
   assert!(writer_result.is_err());
   assert!(matches!(writer_result.unwrap_err(), Error::DatabaseClosed));

   let _ = fs::remove_file(&test_path);
   let _ = fs::remove_file(test_path.with_extension("db-wal"));
   let _ = fs::remove_file(test_path.with_extension("db-shm"));
}

#[tokio::test]
async fn test_memory_databases_never_cached() {
   // :memory: databases should never be cached - each connection is independent
   let db1 = SqliteDatabase::connect(":memory:", None).await.unwrap();
   let db2 = SqliteDatabase::connect(":memory:", None).await.unwrap();

   // Should be different Arc instances (not cached)
   assert!(
      !Arc::ptr_eq(&db1, &db2),
      ":memory: databases should not be cached, each connect should create new instance"
   );

   // Create table in first database
   let mut writer1 = db1.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE test (id INTEGER)")
      .execute(&mut *writer1)
      .await
      .unwrap();

   drop(writer1);

   // Second database should NOT have the table (independent instances)
   let result = sqlx::query("SELECT * FROM test")
      .fetch_optional(db2.read_pool().unwrap())
      .await;

   assert!(
      result.is_err(),
      "Second :memory: database should not have table from first"
   );

   drop(db1);
   drop(db2);
}

#[tokio::test]
async fn test_wal_checkpoint_on_close() {
   use std::fs;

   let test_path = std::env::current_dir()
      .unwrap()
      .join("test_wal_checkpoint.db");

   let db = SqliteDatabase::connect(&test_path, None).await.unwrap();

   // Perform write to initialize WAL mode
   let mut writer = db.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE test (id INTEGER, value TEXT)")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("INSERT INTO test (id, value) VALUES (1, 'test')")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   // WAL file should exist with data
   let wal_path = test_path.with_extension("db-wal");
   assert!(wal_path.exists(), "WAL file should exist after write");

   // Close database (should checkpoint WAL)
   db.close().await.unwrap();

   // WAL file should be either 0 bytes or not exist
   if wal_path.exists() {
      let wal_size = fs::metadata(&wal_path).unwrap().len();
      assert_eq!(wal_size, 0, "WAL file should be 0 bytes after checkpoint");
   }

   let _ = fs::remove_file(&test_path);
   let _ = fs::remove_file(wal_path);
   let _ = fs::remove_file(test_path.with_extension("db-shm"));
}

#[tokio::test]
async fn test_remove() {
   let test_path = std::env::current_dir()
      .unwrap()
      .join("test_close_remove.db");

   let db = SqliteDatabase::connect(&test_path, None).await.unwrap();

   // Perform write to create WAL and SHM files
   let mut writer = db.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE test (id INTEGER)")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   assert!(test_path.exists(), "Database file should exist");

   let wal_path = test_path.with_extension("db-wal");
   let shm_path = test_path.with_extension("db-shm");

   db.remove().await.unwrap();

   // All files should be removed
   assert!(!test_path.exists(), "Database file should be removed");
   assert!(!wal_path.exists(), "WAL file should be removed");
   assert!(!shm_path.exists(), "SHM file should be removed");
}

#[tokio::test]
async fn test_custom_config() {
   let test_path = std::env::current_dir()
      .unwrap()
      .join("test_custom_config.db");

   let custom_config = SqliteDatabaseConfig {
      max_read_connections: 10,
      idle_timeout_secs: 60,
   };

   // Verify custom config is accepted and connection works
   let db = SqliteDatabase::connect(&test_path, Some(custom_config))
      .await
      .unwrap();

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_wal_mode_initialization() {
   let test_path = std::env::current_dir().unwrap().join("test_wal_mode.db");
   let db = SqliteDatabase::connect(&test_path, None).await.unwrap();

   // Before first write, acquire writer which should initialize WAL
   let mut writer = db.acquire_writer().await.unwrap();

   // Check journal mode
   let (mode,): (String,) = sqlx::query_as("PRAGMA journal_mode")
      .fetch_one(&mut *writer)
      .await
      .unwrap();

   assert_eq!(
      mode.to_lowercase(),
      "wal",
      "Journal mode should be WAL after first acquire_writer"
   );

   // Check sync setting
   let (sync,): (i32,) = sqlx::query_as("PRAGMA synchronous")
      .fetch_one(&mut *writer)
      .await
      .unwrap();

   assert_eq!(
      sync, 1,
      "Sync mode should be NORMAL after first acquire_writer"
   );

   drop(writer);

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_db_instance_caching() {
   let test_path = std::env::current_dir().unwrap().join("test_caching.db");

   // Connect twice to same path
   let db1 = SqliteDatabase::connect(&test_path, None).await.unwrap();
   let db2 = SqliteDatabase::connect(&test_path, None).await.unwrap();

   // Should be same Arc instance (cached)
   assert!(
      Arc::ptr_eq(&db1, &db2),
      "Same path should return cached instance"
   );

   drop(db1);
   db2.remove().await.unwrap();
}

#[tokio::test]
async fn test_write_serialization() {
   use std::sync::atomic::{AtomicUsize, Ordering};
   use tokio::sync::Barrier;

   let path = std::env::current_dir()
      .unwrap()
      .join("test_write_serial.db");
   let db = SqliteDatabase::connect(&path, None).await.unwrap();
   sqlx::query("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (0)")
      .execute(&mut *db.acquire_writer().await.unwrap())
      .await
      .unwrap();

   let barrier = Arc::new(Barrier::new(3));
   let (active, max) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));

   let handles: Vec<_> = (0..3)
      .map(|_| {
         let (db, barrier, active, max) = (
            Arc::clone(&db),
            Arc::clone(&barrier),
            Arc::clone(&active),
            Arc::clone(&max),
         );
         tokio::spawn(async move {
            barrier.wait().await;
            let mut w = db.acquire_writer().await.unwrap();
            max.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            sqlx::query("UPDATE t SET v = v + 1")
               .execute(&mut *w)
               .await
               .unwrap();
            active.fetch_sub(1, Ordering::SeqCst);
         })
      })
      .collect();

   for h in handles {
      h.await.unwrap();
   }

   let (v,): (i64,) = sqlx::query_as("SELECT v FROM t")
      .fetch_one(db.read_pool().unwrap())
      .await
      .unwrap();

   assert_eq!(v, 3, "All 3 writes completed");
   assert_eq!(
      max.load(Ordering::SeqCst),
      1,
      "Expected serialized writes (max 1 active), but {} were simultaneous",
      max.load(Ordering::SeqCst)
   );

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_concurrent_reads_and_writes() {
   use std::sync::atomic::{AtomicBool, Ordering};
   use tokio::sync::Barrier;

   let path = std::env::current_dir().unwrap().join("test_read_write.db");
   let db = SqliteDatabase::connect(&path, None).await.unwrap();
   sqlx::query("CREATE TABLE t (v INTEGER)")
      .execute(&mut *db.acquire_writer().await.unwrap())
      .await
      .unwrap();

   let barrier = Arc::new(Barrier::new(2));
   let write_active = Arc::new(AtomicBool::new(false));
   let read_during_write = Arc::new(AtomicBool::new(false));

   let writer_task = {
      let (db, barrier, write_active) = (
         Arc::clone(&db),
         Arc::clone(&barrier),
         Arc::clone(&write_active),
      );
      tokio::spawn(async move {
         barrier.wait().await;
         let mut w = db.acquire_writer().await.unwrap();
         write_active.store(true, Ordering::SeqCst);
         tokio::time::sleep(std::time::Duration::from_millis(20)).await;
         sqlx::query("INSERT INTO t VALUES (1)")
            .execute(&mut *w)
            .await
            .unwrap();
         write_active.store(false, Ordering::SeqCst);
      })
   };

   let reader_task = {
      let (db, barrier, write_active, read_during_write) = (
         Arc::clone(&db),
         Arc::clone(&barrier),
         Arc::clone(&write_active),
         Arc::clone(&read_during_write),
      );
      tokio::spawn(async move {
         barrier.wait().await;
         tokio::time::sleep(std::time::Duration::from_millis(10)).await;
         let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM t")
            .fetch_one(db.read_pool().unwrap())
            .await
            .unwrap();
         if write_active.load(Ordering::SeqCst) {
            read_during_write.store(true, Ordering::SeqCst);
         }
         assert!(count >= 0);
      })
   };

   writer_task.await.unwrap();
   reader_task.await.unwrap();

   assert!(
      read_during_write.load(Ordering::SeqCst),
      "Read did not overlap with write (WAL mode should allow this)"
   );

   db.remove().await.unwrap();
}
