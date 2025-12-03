use std::collections::HashMap;
use std::sync::Arc;

use tauri::{Manager, RunEvent, Runtime, plugin::Builder as PluginBuilder};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

mod commands;
mod decode;
mod error;
mod wrapper;

pub use error::{Error, Result};
pub use wrapper::{DatabaseWrapper, WriteQueryResult};

/// Database instances managed by the plugin.
///
/// This struct maintains a thread-safe map of database paths to their corresponding
/// connection wrappers.
#[derive(Clone, Default)]
pub struct DbInstances(pub Arc<RwLock<HashMap<String, DatabaseWrapper>>>);

/// Builder for the SQLite plugin.
///
/// Use this to configure the plugin and build the plugin instance.
///
/// # Example
///
/// ```ignore
/// use tauri_plugin_sqlite::Builder;
///
/// // In your Tauri app setup:
/// tauri::Builder::default()
///     .plugin(Builder::new().build())
///     .run(tauri::generate_context!())
///     .expect("error while running tauri application");
/// ```
#[derive(Default)]
pub struct Builder;

impl Builder {
   /// Create a new builder instance.
   pub fn new() -> Self {
      Self
   }

   /// Build the plugin with command registration and state management.
   pub fn build<R: Runtime>(self) -> tauri::plugin::TauriPlugin<R> {
      PluginBuilder::<R>::new("sqlite")
         .invoke_handler(tauri::generate_handler![
            commands::load,
            commands::execute,
            commands::execute_transaction,
            commands::fetch_all,
            commands::fetch_one,
            commands::close,
            commands::close_all,
            commands::remove,
         ])
         .setup(|app, _api| {
            app.manage(DbInstances::default());
            debug!("SQLite plugin initialized");
            // Future PR: Possibly handle migrations here
            Ok(())
         })
         .on_event(|app, event| {
            match event {
               RunEvent::ExitRequested { api, code, .. } => {
                  info!("App exit requested (code: {:?}) - closing databases before exit", code);

                  // Prevent immediate exit so we can close connections and checkpoint WAL
                  api.prevent_exit();

                  let app_handle = app.clone();

                  let handle = match tokio::runtime::Handle::try_current() {
                     Ok(h) => h,
                     Err(_) => {
                        warn!("No tokio runtime available for database cleanup");
                        app_handle.exit(code.unwrap_or(0));
                        return;
                     }
                  };

                  let instances = app.state::<DbInstances>().inner().clone();

                  // Spawn a blocking thread to close databases
                  // (block_in_place panics on current_thread runtime)
                  let cleanup_result = std::thread::spawn(move || {
                     handle.block_on(async {
                        let mut guard = instances.0.write().await;
                        let wrappers: Vec<DatabaseWrapper> =
                           guard.drain().map(|(_, v)| v).collect();

                        // Close databases in parallel with timeout
                        let mut set = tokio::task::JoinSet::new();
                        for wrapper in wrappers {
                           set.spawn(async move { wrapper.close().await });
                        }

                        let timeout_result = tokio::time::timeout(
                           std::time::Duration::from_secs(5),
                           async {
                              while let Some(result) = set.join_next().await {
                                 match result {
                                    Ok(Err(e)) => warn!("Error closing database: {:?}", e),
                                    Err(e) => warn!("Database close task panicked: {:?}", e),
                                    Ok(Ok(())) => {}
                                 }
                              }
                           },
                        )
                        .await;

                        if timeout_result.is_err() {
                           warn!("Database cleanup timed out after 5 seconds");
                        } else {
                           debug!("Database cleanup complete");
                        }
                     })
                  })
                  .join();

                  if let Err(e) = cleanup_result {
                     error!("Database cleanup thread panicked: {:?}", e);
                  }

                  app_handle.exit(code.unwrap_or(0));
               }
               RunEvent::Exit => {
                  // ExitRequested should have already closed all databases
                  // This is just a safety check
                  let instances = app.state::<DbInstances>();
                  match instances.0.try_read() {
                     Ok(guard) => {
                        if !guard.is_empty() {
                           warn!(
                              "Exit event fired with {} database(s) still open - cleanup may have been skipped",
                              guard.len()
                           );
                        } else {
                           debug!("Exit event: all databases already closed");
                        }
                     }
                     Err(_) => {
                        warn!("Exit event: could not check database state (lock held - cleanup may still be in progress)");
                     }
                  }
               }
               _ => {
                  // Other events don't require action
               }
            }
         })
         .build()
   }
}

/// Initializes the plugin with default configuration.
pub fn init<R: Runtime>() -> tauri::plugin::TauriPlugin<R> {
   Builder::new().build()
}
