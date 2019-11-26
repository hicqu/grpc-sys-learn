use std::ffi::CStr;
use std::io::Cursor;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::Arc;
use std::{mem, ptr, str};

use futures::task::{self, Task};
use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};
use grpcio::buf::{GrpcByteBuffer, GrpcByteBufferReader, GrpcSlice};
use grpcio_sys::*;
use protobuf::error::ProtobufError;
use protobuf::{CodedInputStream, CodedOutputStream, Message as PbMessage};

#[repr(u8)]
enum ServerPhase {
    Initing = 0,
    Recving = 1,
    RecvingClose = 2,
    Sending = 3,
    SendingMetadata = 4,
    SendingStatus = 5,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct ServerPhasePacked {
    _pending: u8,
    _recving: u8,
    _recving_close: u8,
    _sending: u8,
    _sending_metadata: u8,
    _sending_status: u8,
}

#[repr(C)]
#[repr(packed(8))]
pub struct ServerContext {
    // Must be the first field.
    phase: ServerPhasePacked,

    // Initialized when the call is created on server side.
    call: *mut grpc_call,
    details: grpc_call_details,
    metadata: grpc_metadata_array,

    cancelled: i32,
    closed: bool,
    recv_message: *mut grpc_byte_buffer,
    send_message: *mut grpc_byte_buffer,

    requests: Option<GrpcByteBufferReader>,
    req_task: Option<Task>,

    responses: Vec<grpc_slice>,
    resp_task: Option<Task>,
}

unsafe impl Send for ServerContext {}

unsafe fn alloc_server_context() -> *mut ServerContext {
    let mut ctx = Box::new(ServerContext {
        phase: ServerPhasePacked::default(),
        call: ptr::null_mut(),
        details: mem::MaybeUninit::uninit().assume_init(),
        metadata: mem::MaybeUninit::uninit().assume_init(),

        cancelled: 0,
        closed: false,
        recv_message: ptr::null_mut(),
        send_message: ptr::null_mut(),

        requests: None,
        req_task: None,
        responses: vec![],
        resp_task: None,
    });
    grpc_call_details_init(&mut (*ctx).details);
    grpc_metadata_array_init(&mut (*ctx).metadata);
    Box::into_raw(ctx)
}

unsafe fn tag_to_server_context(tag: *mut std::ffi::c_void) -> (*mut ServerContext, ServerPhase) {
    let addr: isize = mem::transmute(tag);
    let offset = addr % 8;
    let ctx = mem::transmute(tag.offset(-offset));
    let phase = mem::transmute(offset as u8);
    (ctx, phase)
}

unsafe fn server_context_to_tag(
    ctx: *mut ServerContext,
    phase: ServerPhase,
) -> *mut std::ffi::c_void {
    let offset: u8 = mem::transmute(phase);
    let tag: *mut std::ffi::c_void = mem::transmute(ctx);
    tag.offset(-1 * offset as isize)
}

impl ServerContext {
    pub unsafe fn resolve_tag(tag: *mut std::ffi::c_void) {
        let (ctx, phase) = tag_to_server_context(tag);
        match phase {
            ServerPhase::Initing => on_dispatched(ctx),
            ServerPhase::Recving => on_message_received(ctx),
            ServerPhase::RecvingClose => on_close_received(ctx),
            ServerPhase::Sending => on_message_sent(ctx),
            ServerPhase::SendingMetadata => on_metadata_sent(ctx),
            ServerPhase::SendingStatus => on_status_sent(ctx),
        }
    }
}

/// Dispatch the context into its bounded completion quque.
pub unsafe fn dispatch<Req: PbMessage, Resp: PbMessage>(
    ctx: *mut ServerContext,
) -> (RequestStream<Req>, ResponseSink<Resp>) {
    let tag = server_context_to_tag(ctx, ServerPhase::Initing);
    let err = grpc_call_start_batch((*ctx).call, ptr::null(), 0, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        drop(Box::from_raw(ctx));
        panic!("dispatch fail: {:?}", err);
    }
    (RequestStream::new(ctx), ResponseSink::new(ctx))
}

unsafe fn on_dispatched(ctx: *mut ServerContext) {
    // TODO: combine them.
    send_initial_metadata(ctx);
    recv_close_on_server(ctx);
}

unsafe fn send_initial_metadata(ctx: *mut ServerContext) {
    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_SEND_INITIAL_METADATA;
    let sim = &mut op.data.send_initial_metadata;
    sim.count = 1;
    sim.metadata = mem::transmute(gpr_malloc(mem::size_of::<grpc_metadata>()));

    let key = CStr::from_bytes_with_nul(b"kk\0").unwrap().as_ptr();
    let value = CStr::from_bytes_with_nul(b"vv\0").unwrap().as_ptr();
    (*sim.metadata).key = grpc_slice_from_static_string(key);
    (*sim.metadata).value = grpc_slice_from_static_string(value);

    let tag = server_context_to_tag(ctx, ServerPhase::SendingMetadata);
    let err = grpc_call_start_batch((*ctx).call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("send_initial_metadata fail: {:?}", err);
    }
}

unsafe fn on_metadata_sent(ctx: *mut ServerContext) {}

unsafe fn recv_close_on_server(ctx: *mut ServerContext) {
    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_RECV_CLOSE_ON_SERVER;
    op.data.recv_close_on_server.cancelled = &mut (*ctx).cancelled;

    let tag = server_context_to_tag(ctx, ServerPhase::RecvingClose);
    let err = grpc_call_start_batch((*ctx).call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("recv_close_on_server fail: {:?}", err);
    }
}

unsafe fn on_close_received(ctx: *mut ServerContext) {
    (*ctx).closed = true;
    println!("close is received, canceled: {}", (*ctx).cancelled);
}

unsafe fn server_recv_message(ctx: *mut ServerContext) {
    if (*ctx).closed {
        return;
    }
    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_RECV_MESSAGE;
    (*ctx).recv_message = ptr::null_mut();
    op.data.recv_message.recv_message = &mut (*ctx).recv_message;

    let tag = server_context_to_tag(ctx, ServerPhase::Recving);
    let err = grpc_call_start_batch((*ctx).call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("server_recv_message fail: {:?}", err);
    }
}

unsafe fn on_message_received(ctx: *mut ServerContext) {
    let buffer = GrpcByteBuffer::from_raw((*ctx).recv_message);
    (*ctx).requests = Some(GrpcByteBufferReader::new(buffer));
    if let Some(task) = (*ctx).req_task.take() {
        task.notify();
    }
}

pub unsafe fn server_send_message(ctx: *mut ServerContext) {
    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_SEND_MESSAGE;
    op.data.send_message.send_message = (*ctx).send_message;

    let tag = server_context_to_tag(ctx, ServerPhase::Sending);
    let err = grpc_call_start_batch((*ctx).call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("server_send_message fail: {:?}", err);
    }
}

unsafe fn on_message_sent(ctx: *mut ServerContext) {
    grpc_byte_buffer_destroy((*ctx).send_message);
    (*ctx).send_message = ptr::null_mut();
    if let Some(task) = (*ctx).resp_task.take() {
        task.notify();
    }
}

unsafe fn on_status_sent(ctx: *mut ServerContext) {}

#[derive(Debug)]
pub enum Error {
    Cancelled,
    RemoteStopped,
    WireError(ProtobufError),
}

pub struct RequestStream<T: PbMessage> {
    ctx: *mut ServerContext,
    _t: PhantomData<T>,
}

unsafe impl<T: PbMessage> Send for RequestStream<T> {}

impl<T: PbMessage> RequestStream<T> {
    unsafe fn new(ctx: *mut ServerContext) -> Self {
        Self {
            ctx,
            _t: PhantomData::default(),
        }
    }
}

impl<T: PbMessage + Default> Stream for RequestStream<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<T>, Self::Error> {
        unsafe {
            if let Some(mut reader) = (*self.ctx).requests.take() {
                let mut t = T::default();
                let eof = {
                    let mut stream = CodedInputStream::new(&mut reader);
                    if let Err(e) = t.merge_from(&mut stream) {
                        return Err(Error::WireError(e));
                    }
                    stream.eof()
                };
                match eof {
                    Ok(true) => server_recv_message(self.ctx),
                    _ => (*self.ctx).requests = Some(reader),
                }
                return Ok(Async::Ready(Some(t)));
            }
            server_recv_message(self.ctx);
            Ok(Async::NotReady)
        }
    }
}

impl<T: PbMessage> Drop for RequestStream<T> {
    fn drop(&mut self) {
        unsafe {}
    }
}

pub struct ResponseSink<T: PbMessage> {
    ctx: *mut ServerContext,
    _t: PhantomData<T>,
}

unsafe impl<T: PbMessage> Send for ResponseSink<T> {}

impl<T: PbMessage> ResponseSink<T> {
    unsafe fn new(ctx: *mut ServerContext) -> Self {
        Self {
            ctx,
            _t: PhantomData::default(),
        }
    }
}

impl<T: PbMessage + Default> Sink for ResponseSink<T> {
    type SinkItem = T;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        unsafe {
            if (*self.ctx).closed && (*self.ctx).cancelled != 0 {
                return Err(Error::Cancelled);
            } else if (*self.ctx).closed {
                return Err(Error::RemoteStopped);
            }
            let mut buf = Vec::with_capacity(item.compute_size() as usize);
            item.write_to(&mut CodedOutputStream::new(&mut buf))
                .unwrap();
            (*self.ctx)
                .responses
                .push(mem::transmute(GrpcSlice::from(buf)));
            // TODO: limit the batch size.
            Ok(AsyncSink::Ready)
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        unsafe {
            if (*self.ctx).closed && (*self.ctx).cancelled != 0 {
                return Err(Error::Cancelled);
            } else if (*self.ctx).closed {
                return Err(Error::RemoteStopped);
            }
            unsafe {
                let ptr = &mut (*self.ctx).responses[0];
                let len = (*self.ctx).responses.len();
                (*self.ctx).send_message = grpc_raw_byte_buffer_create(ptr, len);
                server_send_message(self.ctx);
                (*self.ctx).responses.clear();
            }
            Ok(Async::NotReady)
        }
    }
}

impl<T: PbMessage> Drop for ResponseSink<T> {
    fn drop(&mut self) {}
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

pub unsafe fn request_call(
    server: *mut grpc_server,
    cq1: *mut grpc_completion_queue,
    cq2: *mut grpc_completion_queue,
) {
    let ctx = alloc_server_context();
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

/*********************************************
impl ServerContext {
    pub fn phase(&self) -> ServerPhase {
        self.phase
    }

    pub unsafe fn on_dispatched(&mut self) {
        send_initial_metadata(self);
        recv_close_on_server(self);

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
        let cancelled = self.cancelled.take().unwrap();
        println!("close is received, canceled: {}", cancelled);
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
        if let Some(recv_message) = self.recv_message.take() {
            unsafe { grpc_byte_buffer_destroy(recv_message) };
        }
    }
}



pub unsafe fn dispatch_server_context(ctx: *mut ServerContext) {
    assert_eq!((*ctx).phase, ServerPhase::Initing);
    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, ptr::null(), 0, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        drop(Box::from_raw(ctx));
        panic!("dispatch_server_context fail: {:?}", err);
    }
}

pub unsafe fn send_initial_metadata(ctx: *mut ServerContext) {
}

pub unsafe fn recv_close_on_server(ctx: *mut ServerContext) {
    (*ctx).phase = ServerPhase::RecvingClose;
    (*ctx).cancelled = Some(0i32);

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_RECV_CLOSE_ON_SERVER;
    op.data.recv_close_on_server.cancelled = (*ctx).cancelled.as_mut().unwrap();

    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("recv_close_on_server fail: {:?}", err);
    }
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
*********************************************/
