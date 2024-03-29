// This file is generated by rust-protobuf 2.6.0. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

#[derive(PartialEq,Clone,Default)]
pub struct M {
    // message fields
    pub s: ::std::string::String,
    pub i: i64,
    // special fields
    pub unknown_fields: ::protobuf::UnknownFields,
    pub cached_size: ::protobuf::CachedSize,
}

impl<'a> ::std::default::Default for &'a M {
    fn default() -> &'a M {
        <M as ::protobuf::Message>::default_instance()
    }
}

impl M {
    pub fn new() -> M {
        ::std::default::Default::default()
    }

    // string s = 1;


    pub fn get_s(&self) -> &str {
        &self.s
    }
    pub fn clear_s(&mut self) {
        self.s.clear();
    }

    // Param is passed by value, moved
    pub fn set_s(&mut self, v: ::std::string::String) {
        self.s = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_s(&mut self) -> &mut ::std::string::String {
        &mut self.s
    }

    // Take field
    pub fn take_s(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.s, ::std::string::String::new())
    }

    // int64 i = 2;


    pub fn get_i(&self) -> i64 {
        self.i
    }
    pub fn clear_i(&mut self) {
        self.i = 0;
    }

    // Param is passed by value, moved
    pub fn set_i(&mut self, v: i64) {
        self.i = v;
    }
}

impl ::protobuf::Message for M {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.s)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.i = tmp;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if !self.s.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.s);
        }
        if self.i != 0 {
            my_size += ::protobuf::rt::value_size(2, self.i, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.s.is_empty() {
            os.write_string(1, &self.s)?;
        }
        if self.i != 0 {
            os.write_int64(2, self.i)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }

    fn new() -> M {
        M::new()
    }

    fn descriptor_static() -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "s",
                    |m: &M| { &m.s },
                    |m: &mut M| { &mut m.s },
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "i",
                    |m: &M| { &m.i },
                    |m: &mut M| { &mut m.i },
                ));
                ::protobuf::reflect::MessageDescriptor::new::<M>(
                    "M",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }

    fn default_instance() -> &'static M {
        static mut instance: ::protobuf::lazy::Lazy<M> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const M,
        };
        unsafe {
            instance.get(M::new)
        }
    }
}

impl ::protobuf::Clear for M {
    fn clear(&mut self) {
        self.s.clear();
        self.i = 0;
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for M {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for M {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x0bdebug.proto\"\x1f\n\x01M\x12\x0c\n\x01s\x18\x01\x20\x01(\tR\x01s\
    \x12\x0c\n\x01i\x18\x02\x20\x01(\x03R\x01iJ\x98\x01\n\x06\x12\x04\0\0\
    \x05\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\n\n\x02\x04\0\x12\x04\x02\0\
    \x05\x01\n\n\n\x03\x04\0\x01\x12\x03\x02\x08\t\n\x0b\n\x04\x04\0\x02\0\
    \x12\x03\x03\x04\x11\n\x0c\n\x05\x04\0\x02\0\x05\x12\x03\x03\x04\n\n\x0c\
    \n\x05\x04\0\x02\0\x01\x12\x03\x03\x0b\x0c\n\x0c\n\x05\x04\0\x02\0\x03\
    \x12\x03\x03\x0f\x10\n\x0b\n\x04\x04\0\x02\x01\x12\x03\x04\x04\x10\n\x0c\
    \n\x05\x04\0\x02\x01\x05\x12\x03\x04\x04\t\n\x0c\n\x05\x04\0\x02\x01\x01\
    \x12\x03\x04\n\x0b\n\x0c\n\x05\x04\0\x02\x01\x03\x12\x03\x04\x0e\x0fb\
    \x06proto3\
";

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe {
        file_descriptor_proto_lazy.get(|| {
            parse_descriptor_proto()
        })
    }
}
