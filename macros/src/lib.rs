use proc_macro::TokenStream;
use quote::quote;
use syn;


#[proc_macro_derive(HelloMacro)]
pub fn hello_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse::<syn::DeriveInput>(input).unwrap();
    let name = ast.ident;
    //eprintln!("{:#?}", ast);
    (quote!{
        impl #name {
            fn hello_macro(&self) {
                println!("Hello, Macro! My name is {} and am {}!", stringify!(#name), self.z);
            }
        }
    }).into()
}