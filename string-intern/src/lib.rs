use std::collections::HashSet;
use std::fmt;
use std::sync::{LazyLock, RwLock};

use polars::prelude::*;
use serde::de::{Deserialize, Deserializer, Visitor};

static INTERNED: LazyLock<RwLock<HashSet<&'static str>>> =
    LazyLock::new(|| RwLock::new(HashSet::new()));

#[derive(Clone, Copy)]
pub struct Intern(&'static str);

impl Intern {
    pub fn new(s: impl AsRef<str>) -> Self {
        let s = s.as_ref();

        // Try read lock first for the common case
        {
            let set = INTERNED.read().unwrap();
            if let Some(&existing) = set.get(s) {
                return Intern(existing);
            }
        }

        // Need to insert - take write lock
        let mut set = INTERNED.write().unwrap();

        // Double-check in case another thread inserted while we waited
        if let Some(&existing) = set.get(s) {
            return Intern(existing);
        }

        // Leak the string to get a &'static str
        let leaked: &'static str = Box::leak(s.to_owned().into_boxed_str());
        set.insert(leaked);
        Intern(leaked)
    }

    /// Create an Intern from an already-static string without allocating.
    /// Use this for string literals to avoid memory leaks.
    pub fn from_static(s: &'static str) -> Self {
        // Try read lock first for the common case
        {
            let set = INTERNED.read().unwrap();
            if let Some(&existing) = set.get(s) {
                return Intern(existing);
            }
        }

        // Need to insert - take write lock
        let mut set = INTERNED.write().unwrap();

        // Double-check in case another thread inserted while we waited
        if let Some(&existing) = set.get(s) {
            return Intern(existing);
        }

        // Already static - no need to leak
        set.insert(s);
        Intern(s)
    }

    pub fn as_str(&self) -> &'static str {
        self.0
    }

    /// Returns the pointer address - useful for debugging interning behavior
    pub fn as_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }
}

// Pointer-based equality - two Interns are equal iff they point to the same address
impl PartialEq for Intern {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.0.as_ptr(), other.0.as_ptr())
    }
}

impl Eq for Intern {}

// Pointer-based hashing for consistency with PartialEq
impl std::hash::Hash for Intern {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_ptr().hash(state);
    }
}

// Ordering still uses string ordering for sensible sort behavior
impl PartialOrd for Intern {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Intern {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Fast path: same pointer means equal
        if std::ptr::eq(self.0.as_ptr(), other.0.as_ptr()) {
            std::cmp::Ordering::Equal
        } else {
            self.0.cmp(other.0)
        }
    }
}

impl fmt::Display for Intern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl fmt::Debug for Intern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Intern({:?})", self.0)
    }
}

impl AsRef<str> for Intern {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl AsRef<std::path::Path> for Intern {
    fn as_ref(&self) -> &std::path::Path {
        std::path::Path::new(self.0)
    }
}

impl std::ops::Deref for Intern {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl From<&str> for Intern {
    fn from(value: &str) -> Self {
        Intern::new(value)
    }
}

impl From<String> for Intern {
    fn from(value: String) -> Self {
        Intern::new(value)
    }
}

impl From<Box<str>> for Intern {
    fn from(value: Box<str>) -> Self {
        Intern::new(&*value)
    }
}

impl From<std::borrow::Cow<'_, str>> for Intern {
    fn from(value: std::borrow::Cow<'_, str>) -> Self {
        Intern::new(value)
    }
}

impl<'de> Deserialize<'de> for Intern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct InternVisitor;

        impl Visitor<'_> for InternVisitor {
            type Value = Intern;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Intern::new(v))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Intern::new(v))
            }
        }

        deserializer.deserialize_str(InternVisitor)
    }
}

// ============ Polars Integration ============

// NOTE: NamedFrom impls for slices of Intern are in the consuming crate (bench-suite-types)
// using wrapper types to satisfy the orphan rule.

/// Extension trait for creating Series from Intern iterators
pub trait InternSeriesExt {
    fn from_interns(name: PlSmallStr, interns: impl IntoIterator<Item = Intern>) -> Series;
}

impl InternSeriesExt for Series {
    fn from_interns(name: PlSmallStr, interns: impl IntoIterator<Item = Intern>) -> Series {
        let strings: Vec<&'static str> = interns.into_iter().map(|i| i.0).collect();
        StringChunked::from_slice(name, &strings).into_series()
    }
}

/// Extension trait for extracting Interns from a string Series
pub trait InternChunkedExt {
    fn to_interns(&self) -> Vec<Option<Intern>>;
    fn to_interns_unwrap(&self) -> Vec<Intern>;
}

impl InternChunkedExt for StringChunked {
    fn to_interns(&self) -> Vec<Option<Intern>> {
        self.iter().map(|opt| opt.map(Intern::new)).collect()
    }

    fn to_interns_unwrap(&self) -> Vec<Intern> {
        self.iter()
            .map(|opt| Intern::new(opt.expect("unexpected null in string column")))
            .collect()
    }
}

/// Extension trait for Series to work with Interns
pub trait SeriesInternExt {
    fn to_interns(&self) -> PolarsResult<Vec<Option<Intern>>>;
    fn to_interns_unwrap(&self) -> PolarsResult<Vec<Intern>>;
}

impl SeriesInternExt for Series {
    fn to_interns(&self) -> PolarsResult<Vec<Option<Intern>>> {
        Ok(self.str()?.to_interns())
    }

    fn to_interns_unwrap(&self) -> PolarsResult<Vec<Intern>> {
        Ok(self.str()?.to_interns_unwrap())
    }
}

/// Collect an iterator of Interns directly into a StringChunked
impl FromIterator<Intern> for StringChunked {
    fn from_iter<I: IntoIterator<Item = Intern>>(iter: I) -> Self {
        let strings: Vec<&'static str> = iter.into_iter().map(|i| i.0).collect();
        StringChunked::from_slice(PlSmallStr::EMPTY, &strings)
    }
}

/// Helper to collect Option<Intern> iterators into StringChunked
pub fn collect_optional_interns(iter: impl IntoIterator<Item = Option<Intern>>) -> StringChunked {
    let strings: Vec<Option<&'static str>> =
        iter.into_iter().map(|opt| opt.map(|i| i.0)).collect();
    StringChunked::from_iter(strings)
}
