use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

/// Gera `ToRedisArgs` (JSON via serde) e `ToSingleRedisArg`.
/// O tipo precisa implementar `serde::Serialize`.
#[proc_macro_derive(ToSingleRedisArg)]
pub fn derive_to_single_redis_arg(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics ::lazy_graphql::redis_notification::ToRedisArgs
            for #name #ty_generics #where_clause
        {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + ::lazy_graphql::redis_notification::RedisWrite,
            {
                let json = ::lazy_graphql::serde_json::to_vec(self)
                    .expect("falha ao serializar para JSON");
                out.write_arg(&json);
            }
        }

        impl #impl_generics ::lazy_graphql::redis_notification::ToSingleRedisArg
            for #name #ty_generics #where_clause
        {}
    }
    .into()
}

/// Aplica todos os derives necessários para uma mensagem de canal Redis.
#[proc_macro_attribute]
pub fn subscription_message(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    quote! {
        #[derive(
            ::lazy_graphql::async_graphql::SimpleObject,
            ::lazy_graphql::__private::serde::Serialize,
            ::lazy_graphql::__private::serde::Deserialize,
            ::lazy_graphql::lazy_graphql_macros::ToSingleRedisArg,
            ::std::fmt::Debug,
            ::std::clone::Clone,
        )]
        #[serde(crate = "::lazy_graphql::__private::serde")]
        #input
    }
    .into()
}
