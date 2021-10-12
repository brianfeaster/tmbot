use proc_macro::TokenStream;
use quote::quote;
use syn::{parse, DeriveInput};


#[proc_macro_derive(Hello)]
pub fn hello_macro(input: TokenStream) -> TokenStream {
    let ast = parse::<DeriveInput>(input).unwrap();
    let name = ast.ident;
    //eprintln!("{:#?}", ast);
    (quote!{
        impl #name {
            fn hello(&self) {
                println!("{} = {:#?}", stringify!(#name), self);
            }
        }
    }).into()
}

/*
use ::macros::*;
use ::std::{
    boxed::Box,
    error::Error};

#[derive(Debug, Hello)]
struct Greetings {
    z:i32,
    err:Box<dyn Error>
}

fn _fun_macros() -> Bresult<()> {
    let g = Greetings{z:42, err:"".into() };
    Err(std::io::Error::from_raw_os_error(0))?;
    Ok(g.hello())
}
*/