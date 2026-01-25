use core::cmp;
use core::error;
use core::fmt::Display;
use serde::{Deserialize, Deserializer, de::Error as DeError};
use std::fmt::Debug;
use std::num::NonZeroU64;

/// A positive, non-zero f64 value.
///
/// Internally stored as `NonZeroU64` for niche optimization,
/// so `Option<PositiveNonZeroF64>` is the same size as `f64`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PositiveNonZeroF64(NonZeroU64);

#[derive(Clone, Copy, Debug)]
pub enum Error {
    Zero,
    Negative(f64),
    NaN,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Zero => write!(f, "Expected a NonZero float"),
            Error::Negative(v) => write!(f, "Expected a Non Negative float got {}", v),
            Error::NaN => write!(f, "Expected a real number, got NaN"),
        }
    }
}

impl error::Error for Error {}

impl PositiveNonZeroF64 {
    pub fn new(val: f64) -> Result<Self, Error> {
        if val.is_nan() {
            Err(Error::NaN)
        } else if val.is_sign_negative() {
            Err(Error::Negative(val))
        } else if val == 0.0 {
            Err(Error::Zero)
        } else {
            // SAFETY: We verified val is positive and non-zero,
            // so its bit representation is non-zero
            Ok(Self(unsafe { NonZeroU64::new_unchecked(val.to_bits()) }))
        }
    }

    /// Creates a new `PositiveNonZeroF64` without checking the value.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `val` is not zero (neither positive nor negative zero)
    /// - `val` is not negative
    /// - `val` is not NaN
    ///
    /// Violating these invariants may cause undefined behavior in code
    /// that relies on the guarantees of `PositiveNonZeroF64`.
    pub unsafe fn new_unchecked(val: f64) -> Self {
        debug_assert!(
            val > 0.0,
            "PositiveNonZeroF64::new_unchecked called with invalid value: {}",
            val
        );
        // SAFETY: Caller guarantees val is positive and non-zero
        unsafe { Self(NonZeroU64::new_unchecked(val.to_bits())) }
    }

    pub fn get(self) -> f64 {
        f64::from_bits(self.0.get())
    }
}

impl Display for PositiveNonZeroF64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl PartialOrd for PositiveNonZeroF64 {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.get().partial_cmp(&other.get())
    }
}

impl Ord for PositiveNonZeroF64 {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        // SAFETY: PositiveNonZeroF64 guarantees no NaN values,
        // so partial_cmp will always return Some
        unsafe { self.get().partial_cmp(&other.get()).unwrap_unchecked() }
    }
}

impl<'de> Deserialize<'de> for PositiveNonZeroF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Self::new(value).map_err(DeError::custom)
    }
}
