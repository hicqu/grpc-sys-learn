use std::ffi::CStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{mem, ptr, slice};

use grpcio_sys::*;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ClientPhase {
    ExchangingMetadata,
    Recving,
    RecvingStatus,
    Sending,
    SendingClose,
}

pub struct ClientContext {
    ref_count: Arc<AtomicUsize>,
    phase: ClientPhase,
    call: *mut grpc_call,

    recv_initial_metadata: Option<grpc_metadata_array>,

    trailing_metadata: Option<grpc_metadata_array>,
    status: Option<grpc_status_code::Type>,
    status_details: Option<grpc_slice>,
}

impl Drop for ClientContext {
    fn drop(&mut self) {
        unsafe {
            grpc_call_unref(self.call);
            if let Some(mut rim) = self.recv_initial_metadata.take() {
                grpc_metadata_array_destroy(&mut rim);
            }
        }
    }
}

impl ClientContext {
    pub fn phase(&self) -> ClientPhase {
        self.phase
    }

    pub fn dup(&self) -> *mut ClientContext {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        let ctx = box ClientContext {
            ref_count: self.ref_count.clone(),
            phase: ClientPhase::ExchangingMetadata,
            call: self.call,
            recv_initial_metadata: None,
            trailing_metadata: None,
            status: None,
            status_details: None,
        };
        Box::into_raw(ctx)
    }

    pub unsafe fn on_metadata_exchanged(&mut self) {
        println!("metadata exchanged, got:");
        let recv_initial_metadata = self.recv_initial_metadata.as_ref().unwrap();
        let ptr = recv_initial_metadata.metadata;
        let count = recv_initial_metadata.count;
        let metadatas = slice::from_raw_parts(ptr, count);
        for metadata in metadatas {
            let key = grpc_slice_to_c_string(metadata.key);
            let value = grpc_slice_to_c_string(metadata.value);
            println!(
                "metadata {:?} -> {:?}",
                CStr::from_ptr(key),
                CStr::from_ptr(value)
            );
            gpr_free(key as *mut std::ffi::c_void);
            gpr_free(value as *mut std::ffi::c_void);
        }
        client_send_message(self);
    }

    pub unsafe fn on_message_sent(&mut self) {
        println!("a message is sent");
        client_send_close(self);
    }

    pub unsafe fn on_close_sent(&mut self) {
        println!("the final close is sent");
        client_recv_status(self);
    }

    pub unsafe fn on_status_received(&mut self) {
        let status = self.status.take().unwrap();
        println!("the status is received: {:?}", status);
    }
}

pub unsafe fn create_client_context(call: *mut grpc_call) -> Box<ClientContext> {
    Box::new(ClientContext {
        ref_count: Arc::new(AtomicUsize::new(1)),
        phase: ClientPhase::ExchangingMetadata,
        call,
        recv_initial_metadata: None,
        trailing_metadata: None,
        status: None,
        status_details: None,
    })
}

pub unsafe fn exchange_initial_metadata(ctx: *mut ClientContext) {
    (*ctx).phase = ClientPhase::ExchangingMetadata;
    (*ctx).recv_initial_metadata = Some(mem::zeroed());
    let rim = (*ctx).recv_initial_metadata.as_mut().unwrap();
    grpc_metadata_array_init(rim);

    let mut ops: [grpc_op; 2] = [mem::zeroed(), mem::zeroed()];
    ops[0].op = grpc_op_type::GRPC_OP_SEND_INITIAL_METADATA;
    ops[0].data.send_initial_metadata = mem::zeroed();
    ops[1].op = grpc_op_type::GRPC_OP_RECV_INITIAL_METADATA;
    ops[1].data.recv_initial_metadata.recv_initial_metadata = rim;

    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, mem::transmute(&ops[0]), 2, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("exchange_initial_metadata fail: {:?}", err);
    }
}

pub unsafe fn client_send_message(ctx: *mut ClientContext) {
    (*ctx).phase = ClientPhase::Sending;

    let content = CStr::from_bytes_with_nul(b"I am content\0").unwrap();
    let mut slice = grpc_slice_from_static_string(content.as_ptr());
    let buffer = grpc_raw_byte_buffer_create(&mut slice, 1);
    grpc_slice_unref(slice);

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_SEND_MESSAGE;
    op.data.send_message.send_message = buffer;

    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("client_send_message fail: {:?}", err);
    }
}

pub unsafe fn client_send_close(ctx: *mut ClientContext) {
    (*ctx).phase = ClientPhase::SendingClose;

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_SEND_CLOSE_FROM_CLIENT;

    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("client_send_close fail: {:?}", err);
    }
}

pub unsafe fn client_recv_status(ctx: *mut ClientContext) {
    (*ctx).phase = ClientPhase::RecvingStatus;

    (*ctx).trailing_metadata = Some(mem::zeroed());
    let metadata = (*ctx).trailing_metadata.as_mut().unwrap();
    grpc_metadata_array_init(metadata);

    (*ctx).status = Some(0i32);
    let status = (*ctx).status.as_mut().unwrap();

    (*ctx).status_details = Some(grpc_empty_slice());
    let details = (*ctx).status_details.as_mut().unwrap();

    let mut op: grpc_op = mem::zeroed();
    op.op = grpc_op_type::GRPC_OP_RECV_STATUS_ON_CLIENT;
    op.data.recv_status_on_client.trailing_metadata = metadata;
    op.data.recv_status_on_client.status = status;
    op.data.recv_status_on_client.status_details = details;

    let call = (*ctx).call;
    let tag = mem::transmute(ctx);
    let err = grpc_call_start_batch(call, &op, 1, tag, ptr::null_mut());
    if err != grpc_call_error::GRPC_CALL_OK {
        panic!("client_recv_status fail: {:?}", err);
    }
}
