mod keys;
mod schema_model;

use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, GenericArgument, Path, Type};

use crate::{keys::PrimaryKey, schema_model::ModelAttributes};

enum DataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    Boolean,
}

#[proc_macro_attribute]
pub fn tonbo_record(args: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = ast.ident.clone();

    let mut combined_derives = vec![quote!(
        tonbo_marco::KeyAttributes,
        Debug,
        PartialEq,
        Eq,
        Clone
    )];

    let additional_attrs = args.to_string();
    if !additional_attrs.is_empty() {
        combined_derives.push(additional_attrs.parse().unwrap());
    }

    let mut primary_key_definitions = None;

    let mut encode_method_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut encode_size_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut decode_method_fields: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut to_ref_init_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut schema_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut ref_fields: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut from_record_batch_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut field_names: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut arrays_init_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut builder_init_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut arrays_get_fields: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut builder_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut builder_finish_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut builder_as_any_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    // only normal fields
    let mut builder_push_some_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    // only normal fields
    let mut builder_push_none_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut builder_size_fields: Vec<proc_macro2::TokenStream> = Vec::new();

    if let Data::Struct(data_struct) = &ast.data {
        if let Fields::Named(fields) = &data_struct.fields {
            for (i, field) in fields.named.iter().enumerate() {
                let field_name = field.ident.as_ref().unwrap();
                let field_array_name =
                    Ident::new(&format!("{}_array", field_name), field_name.span());
                let field_index = i + 2;

                let mut is_string = false;
                let (
                    is_nullable,
                    field_ty,
                    mapped_type,
                    array_ty,
                    as_method,
                    builder_with_capacity_method,
                    builder,
                    size_method,
                ) = match to_data_type(&field.ty) {
                    Some((DataType::UInt8, is_nullable)) => (
                        is_nullable,
                        quote!(u8),
                        quote!(::arrow::datatypes::DataType::UInt8),
                        quote!(::arrow::array::UInt8Array),
                        quote!(as_primitive::<::arrow::datatypes::UInt8Type>()),
                        quote!(::arrow::array::PrimitiveBuilder::<
                            ::arrow::datatypes::UInt8Type,
                        >::with_capacity(capacity)),
                        quote!(::arrow::array::PrimitiveBuilder<::arrow::datatypes::UInt8Type>),
                        quote!(std::mem::size_of_val(self.#field_name.values_slice())),
                    ),
                    Some((DataType::UInt16, is_nullable)) => (
                        is_nullable,
                        quote!(u16),
                        quote!(::arrow::datatypes::DataType::UInt16),
                        quote!(::arrow::array::UInt16Array),
                        quote!(as_primitive::<::arrow::datatypes::UInt16Type>()),
                        quote!(::arrow::array::PrimitiveBuilder::<
                            ::arrow::datatypes::UInt16Type,
                        >::with_capacity(capacity)),
                        quote!(::arrow::array::PrimitiveBuilder<::arrow::datatypes::UInt16Type>),
                        quote!(std::mem::size_of_val(self.#field_name.values_slice())),
                    ),
                    Some((DataType::UInt32, is_nullable)) => (
                        is_nullable,
                        quote!(u32),
                        quote!(::arrow::datatypes::DataType::UInt32),
                        quote!(::arrow::array::UInt32Array),
                        quote!(as_primitive::<::arrow::datatypes::UInt32Type>()),
                        quote!(::arrow::array::PrimitiveBuilder::<
                            ::arrow::datatypes::UInt32Type,
                        >::with_capacity(capacity)),
                        quote!(::arrow::array::PrimitiveBuilder<::arrow::datatypes::UInt32Type>),
                        quote!(std::mem::size_of_val(self.#field_name.values_slice())),
                    ),
                    Some((DataType::UInt64, is_nullable)) => (
                        is_nullable,
                        quote!(u64),
                        quote!(::arrow::datatypes::DataType::UInt64),
                        quote!(::arrow::array::UInt64Array),
                        quote!(as_primitive::<::arrow::datatypes::UInt64Type>()),
                        quote!(::arrow::array::PrimitiveBuilder::<
                            ::arrow::datatypes::UInt64Type,
                        >::with_capacity(capacity)),
                        quote!(::arrow::array::PrimitiveBuilder<::arrow::datatypes::UInt64Type>),
                        quote!(std::mem::size_of_val(self.#field_name.values_slice())),
                    ),

                    Some((DataType::Int8, is_nullable)) => (
                        is_nullable,
                        quote!(i8),
                        quote!(::arrow::datatypes::DataType::Int8),
                        quote!(::arrow::array::Int8Array),
                        quote!(as_primitive::<::arrow::datatypes::Int8Type>()),
                        quote!(::arrow::array::PrimitiveBuilder::<
                            ::arrow::datatypes::Int8Type,
                        >::with_capacity(capacity)),
                        quote!(::arrow::array::PrimitiveBuilder<::arrow::datatypes::Int8Type>),
                        quote!(std::mem::size_of_val(self.#field_name.values_slice())),
                    ),
                    Some((DataType::Int16, is_nullable)) => (
                        is_nullable,
                        quote!(i16),
                        quote!(::arrow::datatypes::DataType::Int16),
                        quote!(::arrow::array::Int16Array),
                        quote!(as_primitive::<::arrow::datatypes::Int16Type>()),
                        quote!(::arrow::array::PrimitiveBuilder::<
                            ::arrow::datatypes::Int16Type,
                        >::with_capacity(capacity)),
                        quote!(::arrow::array::PrimitiveBuilder<::arrow::datatypes::Int16Type>),
                        quote!(std::mem::size_of_val(self.#field_name.values_slice())),
                    ),
                    Some((DataType::Int32, is_nullable)) => (
                        is_nullable,
                        quote!(i32),
                        quote!(::arrow::datatypes::DataType::Int32),
                        quote!(::arrow::array::Int32Array),
                        quote!(as_primitive::<::arrow::datatypes::Int32Type>()),
                        quote!(::arrow::array::PrimitiveBuilder::<
                            ::arrow::datatypes::Int32Type,
                        >::with_capacity(capacity)),
                        quote!(::arrow::array::PrimitiveBuilder<::arrow::datatypes::Int32Type>),
                        quote!(std::mem::size_of_val(self.#field_name.values_slice())),
                    ),
                    Some((DataType::Int64, is_nullable)) => (
                        is_nullable,
                        quote!(i64),
                        quote!(::arrow::datatypes::DataType::Int64),
                        quote!(::arrow::array::Int64Array),
                        quote!(as_primitive::<::arrow::datatypes::Int64Type>()),
                        quote!(::arrow::array::PrimitiveBuilder::<
                            ::arrow::datatypes::Int64Type,
                        >::with_capacity(capacity)),
                        quote!(::arrow::array::PrimitiveBuilder<::arrow::datatypes::Int64Type>),
                        quote!(std::mem::size_of_val(self.#field_name.values_slice())),
                    ),

                    Some((DataType::String, is_nullable)) => {
                        is_string = true;
                        (
                            is_nullable,
                            quote!(String),
                            quote!(::arrow::datatypes::DataType::Utf8),
                            quote!(::arrow::array::StringArray),
                            quote!(as_string::<i32>()),
                            quote!(::arrow::array::StringBuilder::with_capacity(capacity, 0)),
                            quote!(::arrow::array::StringBuilder),
                            quote!(self.#field_name.values_slice().len()),
                        )
                    }
                    Some((DataType::Boolean, is_nullable)) => (
                        is_nullable,
                        quote!(bool),
                        quote!(::arrow::datatypes::DataType::Boolean),
                        quote!(::arrow::array::BooleanArray),
                        quote!(as_boolean()),
                        quote!(::arrow::array::BooleanBuilder::with_capacity(capacity)),
                        quote!(::arrow::array::BooleanBuilder),
                        quote!(self.#field_name.values_slice().len()),
                    ),

                    None => unreachable!(),
                };

                schema_fields.push(quote! {
                    ::arrow::datatypes::Field::new(stringify!(#field_name), #mapped_type, #is_nullable),
                });
                field_names.push(quote! (#field_name,));
                arrays_init_fields.push(quote! {
                    #field_name: ::std::sync::Arc<#array_ty>,
                });
                builder_init_fields.push(quote! {
                    #field_name: #builder_with_capacity_method,
                });
                builder_fields.push(quote! {
                    #field_name: #builder,
                });
                builder_finish_fields.push(quote! {
                    let #field_name = ::std::sync::Arc::new(self.#field_name.finish());
                });
                builder_as_any_fields.push(quote! {
                    ::std::sync::Arc::clone(&#field_name) as ::std::sync::Arc<dyn ::arrow::array::Array>,
                });
                encode_method_fields.push(quote! {
                    ::tonbo::serdes::Encode::encode(&self.#field_name, writer).await.map_err(|err| ::tonbo::record::RecordEncodeError::Encode {
                        field_name: stringify!(#field_name).to_string(),
                        error: Box::new(err),
                    })?;
                });
                encode_size_fields.push(quote! {
                    + self.#field_name.size()
                });
                builder_size_fields.push(quote! {
                    + #size_method
                });

                match ModelAttributes::parse_field(field) {
                    Ok(false) => {
                        match (is_nullable, is_string) {
                            (true, true) => {
                                to_ref_init_fields
                                    .push(quote! { #field_name: self.#field_name.as_deref(), });
                            }
                            (true, false) => {
                                to_ref_init_fields.push(quote! { #field_name: self.#field_name, });
                            }
                            (false, true) => {
                                to_ref_init_fields
                                    .push(quote! { #field_name: Some(&self.#field_name), });
                            }
                            (false, false) => {
                                to_ref_init_fields
                                    .push(quote! { #field_name: Some(self.#field_name), });
                            }
                        }
                        if is_string {
                            ref_fields.push(quote! { pub #field_name: Option<&'r str>, });
                        } else {
                            ref_fields.push(quote! { pub #field_name: Option<#field_ty>, });
                        }
                        if is_nullable {
                            from_record_batch_fields.push(quote! {
                                let mut #field_name = None;

                                if projection_mask.leaf_included(#field_index) {
                                    let #field_array_name = record_batch
                                        .column(column_i)
                                        .#as_method;

                                    use ::arrow::array::Array;
                                    if !#field_array_name.is_null(offset) {
                                        #field_name = Some(#field_array_name.value(offset));
                                    }
                                    column_i += 1;
                                }
                            });
                            arrays_get_fields.push(quote! {
                                use ::arrow::array::Array;
                                let #field_name = (!self.#field_name.is_null(offset) && projection_mask.leaf_included(#field_index))
                                    .then(|| self.#field_name.value(offset));
                            });
                            builder_push_some_fields.push(quote! {
                                match row.#field_name {
                                    Some(#field_name) => self.#field_name.append_value(#field_name),
                                    None => self.#field_name.append_null(),
                                }
                            });
                            builder_push_none_fields.push(quote! {
                                self.#field_name.append_null();
                            });
                            decode_method_fields.push(quote! {
                                let #field_name = Option::<#field_ty>::decode(reader).await.map_err(|err| ::tonbo::record::RecordDecodeError::Decode {
                                    field_name: stringify!(#field_name).to_string(),
                                    error: Box::new(err),
                                })?;
                            });
                        } else {
                            from_record_batch_fields.push(quote! {
                                let mut #field_name = None;

                                if projection_mask.leaf_included(#field_index) {
                                    #field_name = Some(
                                        record_batch
                                            .column(column_i)
                                            .#as_method
                                            .value(offset),
                                    );
                                    column_i += 1;
                                }
                            });
                            arrays_get_fields.push(quote! {
                                let #field_name = projection_mask
                                    .leaf_included(#field_index)
                                    .then(|| self.#field_name.value(offset));
                            });
                            builder_push_some_fields.push(quote! {
                                self.#field_name.append_value(row.#field_name.unwrap());
                            });
                            builder_push_none_fields.push(if is_string {
                                quote!(self.#field_name.append_value("");)
                            } else {
                                quote!(self.#field_name.append_value(Default::default());)
                            });
                            decode_method_fields.push(quote! {
                                let #field_name = Option::<#field_ty>::decode(reader).await.map_err(|err| ::tonbo::record::RecordDecodeError::Decode {
                                    field_name: stringify!(#field_name).to_string(),
                                    error: Box::new(err),
                                })?.unwrap();
                            });
                        }
                    }
                    Ok(true) => {
                        primary_key_definitions = Some(PrimaryKey {
                            name: field_name.clone(),
                            builder_append_value: quote! {
                                self.#field_name.append_value(key.value);
                            },
                            base_ty: field.ty.clone(),
                            index: field_index,
                            fn_key: if is_string {
                                quote!(&self.#field_name)
                            } else {
                                quote!(self.#field_name)
                            },
                        });

                        if is_nullable {
                            return syn::Error::new_spanned(
                                ast.ident,
                                "primary key cannot be nullable",
                            )
                            .to_compile_error()
                            .into();
                        }
                        if is_string {
                            to_ref_init_fields.push(quote! { #field_name: &self.#field_name, });
                            ref_fields.push(quote! { pub #field_name: &'r str, });
                        } else {
                            to_ref_init_fields.push(quote! { #field_name: self.#field_name, });
                            ref_fields.push(quote! { pub #field_name: #field_ty, });
                        }
                        from_record_batch_fields.push(quote! {
                            let #field_name = record_batch
                                .column(column_i)
                                .#as_method
                                .value(offset);
                            column_i += 1;
                        });
                        arrays_get_fields.push(quote! {
                           let #field_name = self.#field_name.value(offset);
                        });
                        decode_method_fields.push(quote! {
                            let #field_name = #field_ty::decode(reader).await.map_err(|err| ::tonbo::record::RecordDecodeError::Decode {
                                field_name: stringify!(#field_name).to_string(),
                                error: Box::new(err),
                            })?;
                        });
                    }
                    Err(err) => return TokenStream::from(err.to_compile_error()),
                }
            }
        }
    } else {
        return syn::Error::new_spanned(ast.ident, "This macro only supports structs")
            .to_compile_error()
            .into();
    }
    let PrimaryKey {
        name: primary_key_name,
        base_ty: primary_key_ty,
        fn_key: fn_primary_key,
        builder_append_value: builder_append_primary_key,
        index: primary_key_index,
    } = primary_key_definitions.unwrap();

    let struct_ref_name = Ident::new(&format!("{}Ref", struct_name), struct_name.span());
    let struct_arrays_name = Ident::new(
        &format!("{}ImmutableArrays", struct_name),
        struct_name.span(),
    );
    let struct_builder_name = Ident::new(&format!("{}Builder", struct_name), struct_name.span());

    let gen = quote! {
        #[derive(#(#combined_derives),*)]
        #ast

        impl ::tonbo::record::Record for #struct_name {
            type Columns = #struct_arrays_name;

            type Key = #primary_key_ty;

            type Ref<'r> = #struct_ref_name<'r>
            where
                Self: 'r;

            fn key(&self) -> <<Self as ::tonbo::record::Record>::Key as ::tonbo::record::Key>::Ref<'_> {
                #fn_primary_key
            }

            fn primary_key_index() -> usize {
                #primary_key_index
            }

            fn primary_key_path() -> (::parquet::schema::types::ColumnPath, Vec<::parquet::format::SortingColumn>) {
                (
                    ::parquet::schema::types::ColumnPath::new(vec!["_ts".to_string(), stringify!(#primary_key_name).to_string()]),
                    vec![::parquet::format::SortingColumn::new(1_i32, true, true), ::parquet::format::SortingColumn::new(#primary_key_index as i32, false, true)]
                )
            }

            fn as_record_ref(&self) -> Self::Ref<'_> {
                #struct_ref_name {
                    #(#to_ref_init_fields)*
                }
            }

            fn arrow_schema() -> &'static ::std::sync::Arc<::arrow::datatypes::Schema> {
                static SCHEMA: ::once_cell::sync::Lazy<::std::sync::Arc<::arrow::datatypes::Schema>> = ::once_cell::sync::Lazy::new(|| {
                    ::std::sync::Arc::new(::arrow::datatypes::Schema::new(vec![
                        ::arrow::datatypes::Field::new("_null", ::arrow::datatypes::DataType::Boolean, false),
                        ::arrow::datatypes::Field::new("_ts", ::arrow::datatypes::DataType::UInt32, false),
                        #(#schema_fields)*
                    ]))
                });

                &SCHEMA
            }
        }

        impl ::tonbo::serdes::Decode for #struct_name {
            type Error = ::tonbo::record::RecordDecodeError;

            async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
            where
                R: ::tokio::io::AsyncRead + Unpin,
            {
                #(#decode_method_fields)*

                Ok(Self {
                    #(#field_names)*
                })
            }
        }

        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        pub struct #struct_ref_name<'r> {
            #(#ref_fields)*
        }

        impl<'r> ::tonbo::record::RecordRef<'r> for #struct_ref_name<'r> {
            type Record = #struct_name;

            fn key(self) -> <<Self::Record as ::tonbo::record::Record>::Key as ::tonbo::record::Key>::Ref<'r> {
                self.#primary_key_name
            }

            fn from_record_batch(
                record_batch: &'r ::arrow::record_batch::RecordBatch,
                offset: usize,
                projection_mask: &'r ::parquet::arrow::ProjectionMask,
            ) -> ::tonbo::record::internal::InternalRecordRef<'r, Self> {
                use ::arrow::array::AsArray;

                let mut column_i = 2;
                let null = record_batch.column(0).as_boolean().value(offset);

                let ts = record_batch
                    .column(1)
                    .as_primitive::<::arrow::datatypes::UInt32Type>()
                    .value(offset)
                    .into();

                #(#from_record_batch_fields)*

                let record = #struct_ref_name {
                    #(#field_names)*
                };
                ::tonbo::record::internal::InternalRecordRef::new(ts, record, null)
            }
        }

        impl<'r> ::tonbo::serdes::Encode for #struct_ref_name<'r> {
            type Error = ::tonbo::record::RecordEncodeError;

            async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
            where
                W: ::tokio::io::AsyncWrite + Unpin + Send,
            {
                #(#encode_method_fields)*

                Ok(())
            }

            fn size(&self) -> usize {
                0 #(#encode_size_fields)*
            }
        }

        #[derive(Debug)]
        pub struct #struct_arrays_name {
            _null: ::std::sync::Arc<::arrow::array::BooleanArray>,
            _ts: ::std::sync::Arc<::arrow::array::UInt32Array>,

            #(#arrays_init_fields)*

            record_batch: ::arrow::record_batch::RecordBatch,
        }

        impl ::tonbo::inmem::immutable::ArrowArrays for #struct_arrays_name {
            type Record = #struct_name;

            type Builder = #struct_builder_name;

            fn builder(capacity: usize) -> Self::Builder {
                #struct_builder_name {
                    #(#builder_init_fields)*

                    _null: ::arrow::array::BooleanBufferBuilder::new(capacity),
                    _ts: ::arrow::array::UInt32Builder::with_capacity(capacity),
                }
            }

            fn get(
                &self,
                offset: u32,
                projection_mask: &::parquet::arrow::ProjectionMask,
            ) -> Option<Option<<Self::Record as ::tonbo::record::Record>::Ref<'_>>> {
                let offset = offset as usize;

                if offset >= ::arrow::array::Array::len(self.#primary_key_name.as_ref()) {
                    return None;
                }
                if self._null.value(offset) {
                    return Some(None);
                }

                #(#arrays_get_fields)*

                Some(Some(#struct_ref_name {
                    #(#field_names)*
                }))
            }

            fn as_record_batch(&self) -> &::arrow::record_batch::RecordBatch {
                &self.record_batch
            }
        }

        pub struct #struct_builder_name {
            #(#builder_fields)*

            _null: ::arrow::array::BooleanBufferBuilder,
            _ts: ::arrow::array::UInt32Builder,
        }

        impl ::tonbo::inmem::immutable::Builder<#struct_arrays_name> for #struct_builder_name {
            fn push(&mut self, key: ::tonbo::timestamp::timestamped::Timestamped<<<#struct_name as ::tonbo::record::Record>::Key as ::tonbo::record::Key>::Ref<'_>>, row: Option<#struct_ref_name>) {
                #builder_append_primary_key
                match row {
                    Some(row) => {
                        #(#builder_push_some_fields)*

                        self._null.append(false);
                        self._ts.append_value(key.ts.into());
                    }
                    None => {
                        #(#builder_push_none_fields)*

                        self._null.append(true);
                        self._ts.append_value(key.ts.into());
                    }
                }
            }

            fn written_size(&self) -> usize {
                0 #(#builder_size_fields)*
            }

            fn finish(&mut self, indices: Option<&[usize]>) -> #struct_arrays_name {
                #(#builder_finish_fields)*

                let _null = ::std::sync::Arc::new(::arrow::array::BooleanArray::new(self._null.finish(), None));
                let _ts = ::std::sync::Arc::new(self._ts.finish());
                let mut record_batch = ::arrow::record_batch::RecordBatch::try_new(
                    ::std::sync::Arc::clone(
                        <<#struct_arrays_name as ::tonbo::inmem::immutable::ArrowArrays>::Record as ::tonbo::record::Record>::arrow_schema(),
                    ),
                    vec![
                        ::std::sync::Arc::clone(&_null) as ::std::sync::Arc<dyn ::arrow::array::Array>,
                        ::std::sync::Arc::clone(&_ts) as ::std::sync::Arc<dyn ::arrow::array::Array>,

                        #(#builder_as_any_fields)*
                    ],
                )
                .expect("create record batch must be successful");
                if let Some(indices) = indices {
                    record_batch = record_batch
                        .project(indices)
                        .expect("projection indices must be successful");
                }

                #struct_arrays_name {
                    #(#field_names)*

                    _null,
                    _ts,
                    record_batch,
                }
            }
        }
    };
    // std::fs::write("../test.rs", gen.to_string()).unwrap();

    gen.into()
}

fn to_data_type(ty: &Type) -> Option<(DataType, bool)> {
    if let Type::Path(type_path) = ty {
        if type_path.path.segments.len() == 1 {
            let segment = &type_path.path.segments[0];
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(ref generic_args) = segment.arguments {
                    if generic_args.args.len() == 1 {
                        return if let GenericArgument::Type(Type::Path(type_path)) =
                            &generic_args.args[0]
                        {
                            Some((path_to_type(&type_path.path), true))
                        } else {
                            None
                        };
                    }
                }
            }
        }
        return Some((path_to_type(&type_path.path), false));
    }
    None
}

fn path_to_type(path: &Path) -> DataType {
    if path.is_ident("u8") {
        DataType::UInt8
    } else if path.is_ident("u16") {
        DataType::UInt16
    } else if path.is_ident("u32") {
        DataType::UInt32
    } else if path.is_ident("u64") {
        DataType::UInt64
    } else if path.is_ident("i8") {
        DataType::Int8
    } else if path.is_ident("i16") {
        DataType::Int16
    } else if path.is_ident("i32") {
        DataType::Int32
    } else if path.is_ident("i64") {
        DataType::Int64
    } else if path.is_ident("String") {
        DataType::String
    } else if path.is_ident("bool") {
        DataType::Boolean
    } else {
        todo!()
    }
}

#[proc_macro_derive(KeyAttributes, attributes(primary_key))]
pub fn key_attributes(_input: TokenStream) -> TokenStream {
    let gen = quote::quote! {};
    gen.into()
}
