#![feature(box_syntax)]
#![feature(let_chains)]

pub mod client_context;
pub mod server_context;

use grpcio_sys::*;
use std::ffi::CStr;
use std::str;

pub unsafe fn grpc_version() -> &'static str {
    let s = CStr::from_ptr(grpc_version_string()).to_bytes();
    str::from_utf8_unchecked(s)
}

pub unsafe fn init_channel_args() -> grpc_channel_args {
    let arg = Box::new(grpc_arg {
        type_: grpc_arg_type::GRPC_ARG_INTEGER,
        key: GRPC_ARG_MAX_CONCURRENT_STREAMS as *const u8 as *mut i8,
        value: grpc_arg_grpc_arg_value { integer: 1024 },
    });

    grpc_channel_args {
        num_args: 1,
        args: Box::into_raw(arg),
    }
}

pub unsafe fn must_check_event_type(e: &grpc_event) {
    let t = (*e).type_;
    if t != grpc_completion_type::GRPC_OP_COMPLETE {
        panic!("unexpected event: {:?}", t);
    }
}

pub unsafe fn grpc_slice_to_rust(s: &grpc_slice) -> &[u8] {
    unsafe {
        if !s.refcount.is_null() {
            let start = s.data.refcounted.bytes;
            let len = s.data.refcounted.length;
            std::slice::from_raw_parts(start, len)
        } else {
            let len = s.data.inlined.length;
            &s.data.inlined.bytes[..len as usize]
        }
    }
}

#[cfg(test)]
mod tests {}
