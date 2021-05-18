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