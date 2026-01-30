#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    FromUTF8(std::string::FromUtf8Error),
    UTF8(std::str::Utf8Error),

}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IO(i) => write!(f, "IO Error: {}", i),
            Error::FromUTF8(i) => write!(f, "UTF8 Error: {}", i),
            Error::UTF8(i) => write!(f, "UTF8 Error: {}", i),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::FromUTF8(value)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::UTF8(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

