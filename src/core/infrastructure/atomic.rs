use std::path::Path;
use anyhow::{Context, Result};

/// Write data to a file atomically via temp file + rename.
/// Prevents corruption if the process crashes mid-write.
pub fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path.parent().context("File path has no parent directory")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("Failed to create directory: {}", parent.display()))?;

    let tmp_path = path.with_extension("tmp");
    std::fs::write(&tmp_path, data)
        .with_context(|| format!("Failed to write temp file: {}", tmp_path.display()))?;

    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("Failed to rename {} -> {}", tmp_path.display(), path.display()))?;

    Ok(())
}
