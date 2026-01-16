use std::io::Read;
use std::mem;
pub enum LazyRead<T>
where T:Read{
    Orig(T),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug)]
pub enum Error{
    IO(std::io::Error),
    UTF8(std::string::FromUtf8Error) 
}

impl std::error::Error for Error{}

impl std::fmt::Display for Error{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self{
            Error::IO(i)=>write!(f,"IO Error:{}",i),
            Error::UTF8(i)=>write!(f,"IO Error:{}",i),
        }
    }
}

impl From<std::io::Error> for Error{
    fn from(value: std::io::Error) -> Self {
        Self::IO(value) 
    }
}

impl From<std::string::FromUtf8Error> for Error{
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::UTF8(value) 
    }
}

type Result<T> = std::result::Result<T,Error>;

impl <T>LazyRead<T>
where T:std::io::Read{
    pub fn new(input:T)->Self{
        Self::Orig(input) 
    }
    pub fn get_string(&mut self)->Result<&str>{
        match self{
            LazyRead::Orig(v)=>{
                let mut tmp_string = String::new();
                v.read_to_string(&mut tmp_string)?;
                *self = LazyRead::String(tmp_string); 
            }
            LazyRead::Bytes(b)=>{
                let tmp = mem::take(b);
                *self = LazyRead::String(String::from_utf8(tmp)?);
            }
            LazyRead::String(_)=>{}

        };

        Ok(if let LazyRead::String(s) = self{
            s 
        }else{
            unreachable!()
        })
    }
    pub fn get_bytes(&mut self)->Result<&[u8]>{
        if let LazyRead::Orig(v) = self{
            let mut b = Vec::new();
            v.read_to_end(&mut b)?;
            *self = LazyRead::Bytes(b);
        }
        
        Ok(match self{
            LazyRead::Bytes(b)=>b,
            LazyRead::String(s)=>s.as_bytes(),
            LazyRead::Orig(_)=>unreachable!()
        })
    }
}
