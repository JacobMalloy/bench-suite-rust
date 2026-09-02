use core::cmp;
use core::error;
use core::fmt::Display;
use serde::{Deserialize, Deserializer, de::Error as DeError};
use std::fmt::Debug;
use std::num::NonZeroU64;

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
            Error::Negative(v) => write!(f, "Expected a Non Negative float got {v}"),
            Error::NaN => write!(f, "Expected a real number, got NaN"),
        }
    }
}

impl error::Error for Error {}

/// The sign bit, set on the stored bits to make the representation non-zero.
const SIGN_BIT: u64 = 1 << 63;

/// A positive f64 value, allowing zero.
///
/// Rejects NaN and negative values (including `-0.0`), so the sign bit of a
/// valid value is always clear. That bit is set in the stored representation to
/// keep it `NonZeroU64` for niche optimization, so `Option<PositiveF64>` is the
/// same size as `f64`.
///
/// This is the only type here that touches the bit representation;
/// [`PositiveNonZeroF64`] wraps it and adds the non-zero check.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PositiveF64(NonZeroU64);

impl PositiveF64 {
    /// Creates a new `PositiveF64` from a `f64` value.
    ///
    /// # Errors
    ///
    /// Returns `Error::NaN` if `val` is NaN.
    /// Returns `Error::Negative` if `val` is negative (including `-0.0`).
    pub fn new(val: f64) -> Result<Self, Error> {
        if val.is_nan() {
            Err(Error::NaN)
        } else if val.is_sign_negative() {
            Err(Error::Negative(val))
        } else {
            // SAFETY: We verified val is non-negative, so its sign bit is clear
            // and setting it yields a non-zero representation.
            Ok(Self(unsafe {
                NonZeroU64::new_unchecked(val.to_bits() | SIGN_BIT)
            }))
        }
    }

    /// Creates a new `PositiveF64` without checking the value.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `val` is not negative (including `-0.0`)
    /// - `val` is not NaN
    ///
    /// Violating these invariants may cause undefined behavior in code
    /// that relies on the guarantees of `PositiveF64`.
    #[must_use]
    pub unsafe fn new_unchecked(val: f64) -> Self {
        debug_assert!(
            val >= 0.0 && !val.is_sign_negative(),
            "PositiveF64::new_unchecked called with invalid value: {val}"
        );
        // SAFETY: Caller guarantees val is non-negative, so setting the sign
        // bit yields a non-zero representation.
        unsafe { Self(NonZeroU64::new_unchecked(val.to_bits() | SIGN_BIT)) }
    }

    #[must_use]
    pub fn get(self) -> f64 {
        f64::from_bits(self.0.get() & !SIGN_BIT)
    }

    /// True when this value is `+0.0`, the only zero this type can hold.
    #[must_use]
    fn is_zero(self) -> bool {
        self.0.get() == SIGN_BIT
    }
}

impl Display for PositiveF64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl PartialOrd for PositiveF64 {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PositiveF64 {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        // SAFETY: PositiveF64 guarantees no NaN values,
        // so partial_cmp will always return Some
        unsafe { self.get().partial_cmp(&other.get()).unwrap_unchecked() }
    }
}

impl<'de> Deserialize<'de> for PositiveF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Self::new(value).map_err(DeError::custom)
    }
}

/// A positive, non-zero f64 value.
///
/// A [`PositiveF64`] that also rejects zero, so it inherits that type's niche
/// optimization: `Option<PositiveNonZeroF64>` is the same size as `f64`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PositiveNonZeroF64(PositiveF64);

impl PositiveNonZeroF64 {
    /// Creates a new `PositiveNonZeroF64` from a `f64` value.
    ///
    /// # Errors
    ///
    /// Returns `Error::NaN` if `val` is NaN.
    /// Returns `Error::Negative` if `val` is negative.
    /// Returns `Error::Zero` if `val` is zero.
    pub fn new(val: f64) -> Result<Self, Error> {
        let val = PositiveF64::new(val)?;
        if val.is_zero() {
            Err(Error::Zero)
        } else {
            Ok(Self(val))
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
    #[must_use]
    pub unsafe fn new_unchecked(val: f64) -> Self {
        debug_assert!(
            val > 0.0,
            "PositiveNonZeroF64::new_unchecked called with invalid value: {val}"
        );
        // SAFETY: Caller guarantees val is positive and non-zero
        unsafe { Self(PositiveF64::new_unchecked(val)) }
    }

    #[must_use]
    pub fn get(self) -> f64 {
        self.0.get()
    }
}

impl From<PositiveNonZeroF64> for PositiveF64 {
    fn from(value: PositiveNonZeroF64) -> Self {
        value.0
    }
}

impl Display for PositiveNonZeroF64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

#[cfg(test)]
mod tests {
    // Exact comparison is the point: these values must round-trip bit-for-bit.
    #![allow(clippy::float_cmp)]

    use super::*;
    use core::mem::size_of;

    #[test]
    fn both_types_keep_the_niche() {
        assert_eq!(size_of::<Option<PositiveF64>>(), size_of::<f64>());
        assert_eq!(size_of::<Option<PositiveNonZeroF64>>(), size_of::<f64>());
    }

    #[test]
    fn round_trips() {
        for val in [0.0, 1.0, 0.5, f64::MIN_POSITIVE, f64::MAX, f64::INFINITY] {
            assert_eq!(PositiveF64::new(val).unwrap().get(), val);
        }
        for val in [1.0, 0.5, f64::MIN_POSITIVE, f64::MAX, f64::INFINITY] {
            assert_eq!(PositiveNonZeroF64::new(val).unwrap().get(), val);
        }
    }

    #[test]
    fn rejects_invalid() {
        assert!(matches!(PositiveF64::new(f64::NAN), Err(Error::NaN)));
        assert!(matches!(PositiveF64::new(-1.0), Err(Error::Negative(_))));
        assert!(matches!(PositiveF64::new(-0.0), Err(Error::Negative(_))));
        assert!(PositiveF64::new(0.0).is_ok());

        assert!(matches!(PositiveNonZeroF64::new(f64::NAN), Err(Error::NaN)));
        assert!(matches!(
            PositiveNonZeroF64::new(-1.0),
            Err(Error::Negative(_))
        ));
        assert!(matches!(
            PositiveNonZeroF64::new(-0.0),
            Err(Error::Negative(_))
        ));
        assert!(matches!(PositiveNonZeroF64::new(0.0), Err(Error::Zero)));
    }

    #[test]
    fn orders_by_value() {
        let mut vals = [4.0, 0.0, 2.5, 1.0].map(|v| PositiveF64::new(v).unwrap());
        vals.sort_unstable();
        assert_eq!(vals.map(PositiveF64::get), [0.0, 1.0, 2.5, 4.0]);

        let mut vals = [4.0, 2.5, 1.0].map(|v| PositiveNonZeroF64::new(v).unwrap());
        vals.sort_unstable();
        assert_eq!(vals.map(PositiveNonZeroF64::get), [1.0, 2.5, 4.0]);
    }
}
