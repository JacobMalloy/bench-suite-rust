use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
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

    // Intentionally no `cargo:rerun-if-changed` directives: emitting none tells
    // cargo to rerun this build script on every build, so the embedded build
    // time always reflects the actual compile time rather than a cached value.
}
