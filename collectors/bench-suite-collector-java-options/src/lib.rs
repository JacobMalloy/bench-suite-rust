use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

// Matches a line from the `-XX:+PrintFlagsFinal` "[Global flags]" dump the JVM prints to
// stdout on startup, e.g.:
//      int ActiveProcessorCount                     = -1                                        {product} {default}
//    ccstr CompilationMode                          = default                                   {product} {default}
// ccstrlist CompileCommand                           =                                           {product} {default}
static OPTION_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(\S+)[ \t]+(\w+)[ \t]*=[ \t]*(\S*)[ \t]*\{([^}]*)\}[ \t]*\{([^}]*)\}").unwrap()
});

#[derive(Debug, Default)]
pub struct BenchSuiteCollectJavaOptions {
    options_df: Option<DataFrame>,
}

impl BenchSuiteCollectJavaOptions {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }

    fn parse(content: &str) -> anyhow::Result<DataFrame> {
        let mut names: Vec<String> = Vec::new();
        let mut types: Vec<String> = Vec::new();
        let mut values: Vec<String> = Vec::new();
        let mut categories: Vec<String> = Vec::new();
        let mut origins: Vec<String> = Vec::new();

        for cap in OPTION_REGEX.captures_iter(content) {
            let value_type = cap.get(1).context("Missing option type")?.as_str();
            let name = cap.get(2).context("Missing option name")?.as_str();
            let value = cap.get(3).context("Missing option value")?.as_str();
            let category = cap.get(4).context("Missing option category")?.as_str();
            let origin = cap.get(5).context("Missing option origin")?.as_str();

            types.push(value_type.to_string());
            names.push(name.to_string());
            values.push(value.to_string());
            categories.push(category.trim().to_string());
            origins.push(origin.trim().to_string());
        }

        df![
            "name" => names,
            "type" => types,
            "value" => values,
            "category" => categories,
            "origin" => origins,
        ]
        .context("Failed to create java options DataFrame")
    }
}

impl BenchSuiteCollect for BenchSuiteCollectJavaOptions {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        let name = file.name();
        if name != "jvm0.stdout"
            && name != "jvm0.txt" // LEGACY: remove once all tests use split files
        {
            return Ok(());
        }

        if self.options_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate stdout files"));
        }

        let content = file.content_string()?;
        self.options_df = Some(Self::parse(content)?);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.options_df {
            rv.push((Intern::from_static("java_options"), df.lazy()));
        }
        Ok(rv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_flags_of_every_kind() {
        let sample = "\
[Global flags]
      int ActiveProcessorCount                     = -1                                        {product} {default}
     bool AlwaysPreTouch                            = false                                     {product} {default}
    ccstr CompilationMode                          = default                                   {product} {default}
ccstrlist CompileCommand                           =                                           {product} {default}
     intx CICompilerCount                          = 12                                        {product} {ergonomic}
     bool DisableExplicitGC                        = true                                      {product} {command line}
     bool ZGenerational                             = true                                      {C2 pd product} {default}
";
        let df = BenchSuiteCollectJavaOptions::parse(sample).unwrap();
        assert_eq!(df.height(), 7);

        let names: Vec<_> = df
            .column("name")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!(names.contains(&"ActiveProcessorCount"));
        assert!(names.contains(&"CompileCommand"));

        let values: Vec<_> = df
            .column("value")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        // ccstrlist CompileCommand has no value on the line
        assert!(values.contains(&""));
        assert!(values.contains(&"-1"));

        let origins: Vec<_> = df
            .column("origin")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!(origins.contains(&"command line"));
        assert!(origins.contains(&"ergonomic"));

        let categories: Vec<_> = df
            .column("category")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!(categories.contains(&"C2 pd product"));
    }

    #[test]
    fn ignores_the_section_header() {
        let df = BenchSuiteCollectJavaOptions::parse("[Global flags]\n").unwrap();
        assert_eq!(df.height(), 0);
    }
}
