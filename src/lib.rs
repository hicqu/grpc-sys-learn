#![feature(box_syntax)]

use std::ffi::CStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
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

pub unsafe fn log_peer_and_method(ctx: *const ServerContext) {
    let peer: *mut i8 = grpc_call_get_peer((*ctx).call);
    let method: *mut i8 = grpc_slice_to_c_string((*ctx).details.method);
    let peer_s = str::from_utf8_unchecked(CStr::from_ptr(peer).to_bytes());
    let method_s = str::from_utf8_unchecked(CStr::from_ptr(method).to_bytes());
    println!("establish a call from {}, method: {}", peer_s, method_s);
    gpr_free(peer as *mut std::ffi::c_void);
    gpr_free(method as *mut std::ffi::c_void);
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ServerPhase {
    Pending,
    Recving,
    Sending,
    SendingMetadata,
    SendingStatus,
    RecvingClose,
}

#[derive(Default)]
struct ServerForward {
    buf: Option<u64>,
    request_stream: Option<SyncSender<u64>>,
    response_sink: Option<Receiver<u64>>,
    notify: Option<*mut ServerContext>,
    active: bool,
}

impl ServerForward {
    fn clear(&mut self) {
        self.request_stream = None;
        self.response_sink = None;
        self.notify = None;
    }
}

static SERVER_CONTEXT_COUNT: AtomicUsize = AtomicUsize::new(0);
const FORWARD_CAPACITY: usize = 5;

pub struct ServerContext {
    ref_count: Arc<AtomicUsize>,

    // Initialized when the call is created on server side.
    call: *mut grpc_call,
    details: grpc_call_details,
    metadata: grpc_metadata_array,

    // For receiving close on server side.
    cancelled: Option<*mut i32>,

    recv_message: Option<*mut grpc_byte_buffer>,

    // For forwarding inputs to outputs.
    forward: ServerForward,

    // For sending status on server side.
    // status: Option<grpc_op_grpc_op_data_grpc_op_send_status_from_server>,
    phase: ServerPhase,
}

impl ServerContext {
    pub fn phase(&self) -> ServerPhase {
        self.phase
    }

    pub fn dup(&self) -> *mut ServerContext {
        SERVER_CONTEXT_COUNT.fetch_add(1, Ordering::SeqCst);
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        Box::into_raw(Box::new(ServerContext {
            ref_count: self.ref_count.clone(),
            call: self.call,
            details: self.details,
            metadata: self.metadata,
            cancelled: None,
            recv_message: None,
            forward: ServerForward::default(),
            phase: ServerPhase::Pending,
        }))
    }

    pub unsafe fn on_dispatched(&mut self) {
        send_initial_metadata(self.dup());
        recv_close_on_server(self.dup());

        let (tx, rx) = mpsc::sync_channel(FORWARD_CAPACITY);
        let ctx = self.dup();
        self.forward.request_stream = Some(tx);
        self.forward.notify = Some(ctx);
        (*ctx).forward.response_sink = Some(rx);
        (*ctx).forward.notify = Some(self);
        server_recv_message(self);
        server_send_message(ctx);
    }

    pub unsafe fn on_message_received(&mut self) {
        println!("on_message_received in {:?}", std::thread::current().name());
        self.forward.active = false;
        assert!(self.forward.buf.is_none());

        let recv_message = self.recv_message.take().unwrap();
        if recv_message.is_null() {
            println!("received an empty");
            server_send_message(self.forward.notify.unwrap());
            self.forward.clear();
            return;
        }

        let ptr = mem::transmute(recv_message);
        match self.forward.request_stream.as_mut().unwrap().try_send(ptr) {
            Ok(_) => server_recv_message(self),
            Err(TrySendError::Full(ptr)) => {
                self.forward.buf = Some(ptr);
                self.forward.active = false;
            }
            _ => {
                println!("handle messages internal error");
                self.forward.clear();
                return;
            }
        }
        server_send_message(self.forward.notify.unwrap());
    }

    pub unsafe fn on_message_sent(&mut self) {
        println!("on_message_sent in {:?}", std::thread::current().name());
        self.forward.active = false;
        server_send_message(self);
    }

    pub unsafe fn on_metadata_sent(&mut self) {
        println!("metadata is sent");
    }

    pub unsafe fn on_status_sent(&mut self) {
        println!("status is sent");
        let ctx: *mut ServerContext = mem::transmute(self.forward.notify.unwrap());
        drop(Box::from_raw(ctx));
    }

    pub unsafe fn on_close_received(&mut self) {
        println!("close is received");
    }
}

impl Drop for ServerContext {
    fn drop(&mut self) {
        SERVER_CONTEXT_COUNT.fetch_sub(1, Ordering::SeqCst);
        if self.ref_count.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        println!("----- release a server context");
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

pub unsafe fn request_call(
    server: *mut grpc_server,
    cq1: *mut grpc_completion_queue,
    cq2: *mut grpc_completion_queue,
) {
    SERVER_CONTEXT_COUNT.fetch_add(1, Ordering::SeqCst);
    println!("----- request a server context");

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
        forward: ServerForward::default(),
        phase: ServerPhase::Pending,
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

pub unsafe fn dispatch_server_context(ctx: *mut ServerContext) {
    assert_eq!((*ctx).phase, ServerPhase::Pending);
    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, ptr::null(), 0, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        drop(Box::from_raw(ctx));
        panic!("dispatch_server_context fail: {:?}", err);
    }
}

pub unsafe fn send_initial_metadata(ctx: *mut ServerContext) {
    (*ctx).phase = ServerPhase::SendingMetadata;

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

    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("send_initial_metadata fail: {:?}", err);
    }
}

pub unsafe fn recv_close_on_server(ctx: *mut ServerContext) {
    (*ctx).phase = ServerPhase::RecvingClose;

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_RECV_CLOSE_ON_SERVER;
    let cancelled = Box::into_raw(Box::new(0i32));
    (*ctx).cancelled = Some(cancelled);
    op.data.recv_close_on_server.cancelled = cancelled;

    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("recv_close_on_server fail: {:?}", err);
    }
}

pub unsafe fn server_recv_message(ctx: *mut ServerContext) {
    (*ctx).phase = ServerPhase::Recving;
    if (*ctx).forward.active || (*ctx).forward.request_stream.is_none() {
        return;
    }
    (*ctx).forward.active = true;
    println!(
        "server_recv_message starting batch in {:?}",
        std::thread::current().name()
    );

    if let Some(ptr) = (*ctx).forward.buf.take() {
        let tx = (*ctx).forward.request_stream.as_ref().unwrap();
        // must success.
        tx.send(ptr).unwrap();
    }

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_RECV_MESSAGE;
    (*ctx).recv_message = Some(ptr::null_mut());
    op.data.recv_message.recv_message = (*ctx).recv_message.as_mut().unwrap();

    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("server_recv_message fail: {:?}", err);
    }
}

pub unsafe fn server_send_message(ctx: *mut ServerContext) {
    (*ctx).phase = ServerPhase::Sending;
    if (*ctx).forward.active {
        return;
    }

    let response_sink = (*ctx).forward.response_sink.as_ref().unwrap();
    let ptr = match response_sink.try_recv() {
        Ok(ptr) => ptr,
        Err(TryRecvError::Empty) => return,
        Err(TryRecvError::Disconnected) => {
            send_status_from_server(ctx);
            return;
        }
    };

    (*ctx).forward.active = true;
    println!(
        "server_send_message starting batch in {:?}",
        std::thread::current().name()
    );

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_SEND_MESSAGE;
    op.data.send_message.send_message = mem::transmute(ptr);

    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("server_recv_message fail: {:?}", err);
    }

    let notify = mem::transmute((*ctx).forward.notify.unwrap());
    server_recv_message(notify);
}

pub unsafe fn send_status_from_server(ctx: *mut ServerContext) {
    (*ctx).phase = ServerPhase::SendingStatus;

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_SEND_STATUS_FROM_SERVER;

    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("send_status_from_server fail: {:?}", err);
    }
}

pub enum ClientPhase {
    Recving,
    Sending,
    RecvingMetadata,
    SendingClose,
}

#[cfg(test)]
mod tests {}
