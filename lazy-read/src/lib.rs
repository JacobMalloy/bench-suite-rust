use std::io::Read;
use std::io::Result;
pub enum LazyRead<T>
where T:Read{
    Orig(T),
    String(String),
}


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
            LazyRead::String(_)=>{}

        };

        Ok(if let LazyRead::String(s) = self{
            s 
        }else{
            unreachable!()
        })
    }
}
