use std::ffi::CString;
use std::{i64, mem, ptr};

use grpcio_sys::*;

use grpc_sys_learn::{client_context::*, *};

unsafe fn main_thread() {
    println!("grpc version: {}", grpc_version());

    let addr = CString::new(b"localhost:44444".to_vec()).unwrap();
    let method = CString::new(b"rpc_1".to_vec()).unwrap();

    let channel_args = init_channel_args();
    let ch = grpc_insecure_channel_create(addr.as_ptr(), &channel_args, ptr::null_mut());

    let cq = grpc_completion_queue_create_for_next(ptr::null_mut());
    let method = grpc_slice_from_static_string(method.as_ptr());
    let host = grpc_slice_from_static_string(addr.as_ptr());
    // let host = grpc_slice_malloc(128);

    let mut deadline: gpr_timespec = mem::zeroed();
    deadline.tv_sec = i64::MAX;
    let call = grpc_channel_create_call(
        ch,
        ptr::null_mut(),
        0,
        cq,
        method,
        &host,
        deadline,
        ptr::null_mut(),
    );

    let ctx = Box::into_raw(create_client_context(call));
    exchange_initial_metadata(ctx);
    client_recv_status((*ctx).dup());
    loop {
        let ev = grpc_completion_queue_next(cq, deadline, ptr::null_mut());
        must_check_event_type(&ev);
        let ctx = ev.tag as *mut ClientContext;
        match (*ctx).phase() {
            ClientPhase::ExchangingMetadata => (*ctx).on_metadata_exchanged(),
            ClientPhase::Sending => (*ctx).on_message_sent(),
            ClientPhase::SendingClose => (*ctx).on_close_sent(),
            ClientPhase::Recving => {}
            ClientPhase::RecvingStatus => {
                (*ctx).on_status_received();
                drop(Box::from_raw(ctx));
            }
        }
    }
}

fn main() {
    unsafe {
        grpc_init();
        main_thread();
        grpc_shutdown();
    }
}
