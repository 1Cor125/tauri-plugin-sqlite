use std::collections::HashMap;
use std::future::Future;

use serde::Deserialize;
use tauri::{Runtime, plugin::Builder as PluginBuilder};
use tokio::sync::RwLock;

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
#[derive(Default)]
pub struct DbInstances(pub RwLock<HashMap<String, DatabaseWrapper>>);

/// Plugin configuration.
///
/// Defines databases to preload during plugin initialization.
#[derive(Default, Clone, Deserialize)]
pub struct PluginConfig {
   /// List of database paths to load on plugin initialization
   #[serde(default)]
   #[allow(dead_code)] // Will be used in future PR
   preload: Vec<String>,
}

/// Helper function to run async commands in both async and sync contexts.
///
/// This handles the case where we're already in a Tokio runtime (use `block_in_place`)
/// or need to create one (use Tauri's async runtime).
#[allow(dead_code)] // Will be used in a future PR
fn run_async_command<F: Future>(cmd: F) -> F::Output {
   if tokio::runtime::Handle::try_current().is_ok() {
      tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(cmd))
   } else {
      tauri::async_runtime::block_on(cmd)
   }
}

/// Builder for the SQLite plugin.
///
/// Use this to configure the plugin and build the plugin instance.
///
/// # Example
///
/// ```rust,ignore
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

   /// Build the plugin.
   ///
   /// Full implementation with preload and lifecycle hooks will be added in a future PR.
   pub fn build<R: Runtime>(self) -> tauri::plugin::TauriPlugin<R, Option<PluginConfig>> {
      // Future PR: Full implementation with setup, preload, and cleanup hooks
      PluginBuilder::<R, Option<PluginConfig>>::new("sqlite")
         .setup(|_app, _api| {
            // Future PR: Database preloading and lifecycle management
            Ok(())
         })
         .build()
   }
}

/// Initializes the plugin with default configuration.
///
/// For custom configuration, use `Builder` instead.
pub fn init<R: Runtime>() -> tauri::plugin::TauriPlugin<R, Option<PluginConfig>> {
   Builder::new().build()
}
