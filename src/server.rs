use std::ffi::CString;
use std::{i64, mem, ptr, thread};

use grpcio_sys::*;

use grpc_sys_learn::{server_context::*, *};

struct ServerRuntime {
    server: *mut grpc_server,
    cq: *mut grpc_completion_queue,
}

impl ServerRuntime {
    fn new(server: *mut grpc_server, cq: *mut grpc_completion_queue) -> Self {
        ServerRuntime { server, cq }
    }
}

unsafe impl Sync for ServerRuntime {}
unsafe impl Send for ServerRuntime {}

unsafe fn main_thread() {
    println!("grpc version: {}", grpc_version());

    let channel_args = init_channel_args();

    let server = grpc_server_create(&channel_args, ptr::null_mut());
    let addr = CString::new(b"localhost:44444".to_vec()).unwrap();
    if grpc_server_add_insecure_http2_port(server, addr.as_ptr()) == 0 {
        eprintln!("bind fail");
        return;
    }

    let cq_for_call = grpc_completion_queue_create_for_next(ptr::null_mut());
    grpc_server_register_completion_queue(server, cq_for_call, ptr::null_mut());
    grpc_server_start(server);

    let runtime = ServerRuntime::new(server, cq_for_call);
    let th = thread::Builder::new()
        .name("dispatcher".to_owned())
        .spawn(move || cq_dispatch_routine(runtime))
        .unwrap();
    th.join().unwrap();
}

unsafe fn cq_dispatch_routine(runtime: ServerRuntime) {
    let cq_bounded = grpc_completion_queue_create_for_next(ptr::null_mut());
    let h_runtime = ServerRuntime::new(ptr::null_mut(), cq_bounded);
    thread::Builder::new()
        .name("handler".to_owned())
        .spawn(move || cq_handler(h_runtime))
        .unwrap();

    let ServerRuntime { server, cq } = runtime;

    let slots = 1;
    (0..slots).for_each(|_| request_call(server, cq_bounded, cq));

    let mut deadline: gpr_timespec = mem::zeroed();
    deadline.tv_sec = i64::MAX;
    loop {
        let ev = grpc_completion_queue_next(cq, deadline, ptr::null_mut());
        must_check_event_type(&ev);
        let ctx = ev.tag as *mut ServerContext;
        log_peer_and_method(ctx);
        dispatch_server_context(ctx);
        request_call(server, cq_bounded, cq);
    }
}

unsafe fn cq_handler(runtime: ServerRuntime) {
    let ServerRuntime { cq, .. } = runtime;

    let mut deadline: gpr_timespec = mem::zeroed();
    deadline.tv_sec = i64::MAX;
    loop {
        let ev = grpc_completion_queue_next(cq, deadline, ptr::null_mut());
        must_check_event_type(&ev);
        let mut ctx = Box::from_raw(ev.tag as *mut ServerContext);
        match ctx.phase() {
            ServerPhase::SendingMetadata => ctx.on_metadata_sent(),
            ServerPhase::RecvingClose => ctx.on_close_received(),
            ServerPhase::SendingStatus => ctx.on_status_sent(),
            ServerPhase::Pending => {
                ctx.on_dispatched();
                mem::forget(ctx);
            }
            ServerPhase::Recving => {
                ctx.on_message_received();
                mem::forget(ctx);
            }
            ServerPhase::Sending => {
                ctx.on_message_sent();
                mem::forget(ctx);
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
