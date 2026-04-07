use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use string_intern::Intern;

#[derive(Debug, Default)]
pub struct BenchSuiteCollectGitInfo {
    git_info_df: Option<DataFrame>,
}

impl BenchSuiteCollectGitInfo {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectGitInfo {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        if file.name() != "git_info.csv" {
            return Ok(());
        }

        if self.git_info_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate git_info.csv files"));
        }

        let cursor = std::io::Cursor::new(file.content_bytes()?);

        let df = CsvReadOptions::default()
            .with_has_header(true)
            .into_reader_with_file_handle(cursor)
            .finish()
            .context("Failed to parse git_info.csv")?;

        self.git_info_df = Some(df);
        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.git_info_df {
            rv.push((Intern::from_static("git_info"), df.lazy()));
        }
        Ok(rv)
    }
}
