use std::ffi::CStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::{mem, ptr, str};

use grpcio_sys::*;

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

pub unsafe fn log_peer_and_method(ctx: &ServerContext) {
    let peer: *mut i8 = grpc_call_get_peer(ctx.call);
    let method: *mut i8 = grpc_slice_to_c_string(ctx.details.method);
    let peer_s = str::from_utf8_unchecked(CStr::from_ptr(peer).to_bytes());
    let method_s = str::from_utf8_unchecked(CStr::from_ptr(method).to_bytes());
    println!("establish a call from {}, method: {}", peer_s, method_s);
    gpr_free(peer as *mut std::ffi::c_void);
    gpr_free(method as *mut std::ffi::c_void);
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum ServerPhase {
    Recving,
    Sending,
    NotifyToSend,
    SendingMetadata,
    SendingStatus,
    RecvingClose,
}

pub enum ClientPhase {
    Recving,
    Sending,
    RecvingMetadata,
    SendingClose,
}

static SERVER_CONTEXT_COUNT: AtomicUsize = AtomicUsize::new(0);

pub struct ServerContext {
    ref_count: Arc<AtomicUsize>,

    // Initialized when the call is created on server side.
    call: *mut grpc_call,
    details: grpc_call_details,
    metadata: grpc_metadata_array,

    // For receiving close on server side.
    cancelled: Option<*mut i32>,

    recv_message: Option<*mut grpc_byte_buffer>,
    request_stream: Option<SyncSender<u64>>,
    response_sink: Option<Receiver<u64>>,

    // For sending status on server side.
    // status: Option<grpc_op_grpc_op_data_grpc_op_send_status_from_server>,
    phase: ServerPhase,
}

impl ServerContext {
    pub fn phase(&self) -> ServerPhase {
        self.phase
    }

    pub fn on_message_received(&mut self) {
        let recv_message = self.recv_message.take();
        if recv_message.is_none() {
            println!("received an empty, sending status");
        // TODO: send status.
        } else {
        }
    }
}

impl Drop for ServerContext {
    fn drop(&mut self) {
        if self.ref_count.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        unsafe {
            grpc_call_unref(self.call);
            grpc_call_details_destroy(&mut self.details);
            grpc_metadata_array_destroy(&mut self.metadata);
        }
        if let Some(cancelled) = self.cancelled.take() {
            drop(unsafe { Box::from_raw(cancelled) });
        }
        if let Some(recv_message) = self.recv_message.take() {
            unsafe { grpc_byte_buffer_destroy(recv_message) };
        }
    }
}

impl Clone for ServerContext {
    fn clone(&self) -> ServerContext {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        ServerContext {
            ref_count: self.ref_count.clone(),
            call: self.call,
            details: self.details,
            metadata: self.metadata,
            cancelled: None,
            recv_message: None,
            request_stream: None,
            response_sink: None,
            phase: ServerPhase::Recving,
        }
    }
}

pub unsafe fn request_call(
    server: *mut grpc_server,
    cq1: *mut grpc_completion_queue,
    cq2: *mut grpc_completion_queue,
) {
    SERVER_CONTEXT_COUNT.fetch_add(1, Ordering::SeqCst);
    let mut details: grpc_call_details = mem::zeroed();
    grpc_call_details_init(&mut details);
    let mut metadata: grpc_metadata_array = mem::zeroed();
    grpc_metadata_array_init(&mut metadata);

    let ctx = Box::new(ServerContext {
        ref_count: Arc::new(AtomicUsize::new(1)),
        call: ptr::null_mut(),
        details,
        metadata,
        cancelled: None,
        recv_message: None,
        request_stream: None,
        response_sink: None,
        phase: ServerPhase::Recving,
    });
    let ctx = Box::into_raw(ctx);
    let err = grpc_server_request_call(
        server,
        &mut (*ctx).call,
        &mut (*ctx).details,
        &mut (*ctx).metadata,
        cq1,
        cq2,
        ctx as *mut std::ffi::c_void,
    );
    if err != grpc_call_error::GRPC_CALL_OK {
        drop(Box::from_raw(ctx));
        panic!("grpc_server_request_call fail: {:?}", err);
    }
}

pub unsafe fn send_initial_metadata(ctx: &mut ServerContext) {
    ctx.phase = ServerPhase::SendingMetadata;

    let mut metadata: grpc_op_grpc_op_data_grpc_op_send_initial_metadata = mem::zeroed();
    metadata.count = 1;
    metadata.metadata = mem::transmute(gpr_malloc(mem::size_of::<grpc_metadata>()));
    let key = CStr::from_bytes_with_nul(b"kk\0").unwrap().as_ptr();
    let value = CStr::from_bytes_with_nul(b"vv\0").unwrap().as_ptr();
    (*metadata.metadata).key = grpc_slice_from_static_string(key);
    (*metadata.metadata).value = grpc_slice_from_static_string(value);

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_SEND_INITIAL_METADATA;
    op.data.send_initial_metadata = metadata;

    let call = ctx.call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("grpc_call_start_batch fail: {:?}", err);
    }
}

pub unsafe fn recv_close_on_server(ctx: &mut ServerContext) {
    ctx.phase = ServerPhase::RecvingClose;

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_RECV_CLOSE_ON_SERVER;
    let cancelled = Box::into_raw(Box::new(0i32));
    ctx.cancelled = Some(cancelled);
    op.data.recv_close_on_server.cancelled = cancelled;

    let call = ctx.call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("grpc_call_start_batch fail: {:?}", err);
    }
}

pub unsafe fn server_recv_message(ctx: &mut ServerContext) {
    ctx.phase = ServerPhase::Recving;

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_RECV_MESSAGE;
    ctx.recv_message = Some(ptr::null_mut());
    op.data.recv_message.recv_message = ctx.recv_message.as_mut().unwrap();

    let call = ctx.call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("grpc_call_start_batch fail: {:?}", err);
    }
}

#[cfg(test)]
mod tests {}
