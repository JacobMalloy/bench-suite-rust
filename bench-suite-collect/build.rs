use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let workspace_root = manifest_dir
        .parent()
        .expect("package directory has no parent")
        .to_path_buf();

    watch_workspace_sources(&manifest_dir, &workspace_root);

    let git_hash = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    let build_time_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    println!("cargo:rustc-env=BENCH_SUITE_GIT_HASH={git_hash}");
    println!("cargo:rustc-env=BENCH_SUITE_BUILD_TIME_MS={build_time_ms}");
}

/// Tells cargo to re-run this script whenever any crate in the workspace
/// changes, so the build id it embeds is never stale.
///
/// Emitting *no* `rerun-if-changed` does not mean "always re-run" - it means
/// "re-run when a file in this package changes", which misses rebuilds driven
/// by a sibling crate. Naming the sources explicitly is what actually covers
/// them. Cargo scans a directory path recursively, so watching each crate's
/// `src` also catches files being added or removed.
///
/// Note the inverse hazard of listing paths at all: it *replaces* the default
/// whole-package rule, so this package's own `Cargo.toml`, `src` and `build.rs`
/// have to be in the list too.
fn watch_workspace_sources(manifest_dir: &Path, workspace_root: &Path) {
    let mut watched = vec![
        workspace_root.join("Cargo.toml"),
        workspace_root.join("Cargo.lock"),
        manifest_dir.join("build.rs"),
    ];

    // Every directory holding a Cargo.toml is a crate of this project. The
    // workspace `members` list is not enough: most crates here are pulled in
    // as path dependencies instead of being listed.
    let entries = std::fs::read_dir(workspace_root).expect("failed to read workspace root");
    let mut crate_dirs: Vec<PathBuf> = entries
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            path.join("Cargo.toml").is_file().then_some(path)
        })
        .collect();
    crate_dirs.sort();

    for crate_dir in crate_dirs {
        watched.push(crate_dir.join("Cargo.toml"));
        let src = crate_dir.join("src");
        if src.is_dir() {
            watched.push(src);
        }
    }

    for path in watched {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}
