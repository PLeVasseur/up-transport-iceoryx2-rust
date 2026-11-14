// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0

use iceoryx2::prelude::{Config, SemanticString};
use iceoryx2_bb_system_types::{file_name::FileName, path::Path};
use up_rust_core::transport::BindingRefusal;

/// Initialization-only native configuration. Frame capacities come from routes.
#[derive(Clone, Debug, Default)]
pub struct Iceoryx2Config {
    /// Native node, service and capacity settings, frozen when setup is created.
    pub native: Config,
}

impl Iceoryx2Config {
    /// Selects the native namespace without changing process-global defaults.
    ///
    /// # Errors
    /// Refuses relative root paths, empty prefixes and invalid native strings.
    pub fn with_namespace(mut self, root: &str, prefix: &str) -> Result<Self, BindingRefusal> {
        if !std::path::Path::new(root).is_absolute() || prefix.is_empty() {
            return Err(BindingRefusal(
                "namespace needs an absolute root and a nonempty prefix",
            ));
        }
        let root =
            Path::new(root.as_bytes()).map_err(|_| BindingRefusal("invalid native root path"))?;
        let prefix = FileName::new(prefix.as_bytes())
            .map_err(|_| BindingRefusal("invalid native namespace prefix"))?;
        self.native.global.set_root_path(&root);
        self.native.global.prefix = prefix;
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<(), BindingRefusal> {
        let limits = &self.native.defaults.publish_subscribe;
        if limits.max_publishers == 0
            || limits.max_subscribers == 0
            || self.native.defaults.event.max_notifiers == 0
        {
            return Err(BindingRefusal(
                "native publisher/subscriber/notifier capacities must be nonzero",
            ));
        }
        crate::sizing::local_service_capacities(&self.native)
    }
}
