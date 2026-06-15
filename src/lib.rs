use std::{collections::VecDeque, io::Cursor, mem::MaybeUninit, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;
use up_rust::{
    LoanedPayload, PayloadLoanProvenance, PreparedTxLoanSpec, UCode, UEncodedLoanedRxFrame,
    UEncodedRxFrame, UEncodedZeroCopyListener, UFrameMetadata, UStatus, UTxBuffer, UUninitTxBuffer,
    UUri, UVecTxBuffer, UVecUninitTxBuffer, UWire, UWireTransport, UWithWire,
    UZeroCopyTransportCore, UZeroCopyUninitTransportCore,
};

/// uProtocol major version mirrored in the iceoryx2 user header.
pub const UPROTOCOL_MAJOR_VERSION: u8 = 0;

/// Minimal physical mirror of the iceoryx2 user header fields needed by the proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UProtocolHeader {
    pub uprotocol_major_version: u8,
    pub metadata_len: u64,
    pub payload_len: u64,
    pub payload_alignment: u64,
}

impl UProtocolHeader {
    fn from_prepared(spec: &PreparedTxLoanSpec) -> Result<Self, UStatus> {
        Ok(Self {
            uprotocol_major_version: UPROTOCOL_MAJOR_VERSION,
            metadata_len: spec.encoded_metadata().len().try_into().map_err(|_| {
                UStatus::fail_with_code(UCode::InvalidArgument, "metadata length overflow")
            })?,
            payload_len: spec.payload_len().try_into().map_err(|_| {
                UStatus::fail_with_code(UCode::InvalidArgument, "payload length overflow")
            })?,
            payload_alignment: spec.payload_alignment().try_into().map_err(|_| {
                UStatus::fail_with_code(UCode::InvalidArgument, "payload alignment overflow")
            })?,
        })
    }

    fn validate_against(&self, metadata_len: usize, payload_len: usize) -> Result<(), UStatus> {
        if self.uprotocol_major_version != UPROTOCOL_MAJOR_VERSION {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "unsupported uProtocol major version",
            ));
        }
        if usize::try_from(self.metadata_len).ok() != Some(metadata_len) {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "metadata length mirror mismatch",
            ));
        }
        if usize::try_from(self.payload_len).ok() != Some(payload_len) {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "payload length mirror mismatch",
            ));
        }
        Ok(())
    }
}

/// Physical iceoryx2 core proof that consumes prepared metadata bytes.
#[derive(Clone, Default)]
pub struct Iceoryx2WireCore {
    state: Arc<Mutex<Iceoryx2WireState>>,
}

#[derive(Default)]
struct Iceoryx2WireState {
    prepared: Vec<PreparedTxLoanSpec>,
    received: VecDeque<Iceoryx2EncodedRxFrame>,
    listeners: Vec<Arc<dyn UEncodedZeroCopyListener<Iceoryx2EncodedRxFrame>>>,
}

impl Iceoryx2WireCore {
    /// Creates an empty proof core.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Wraps this core in the generic wire adapter.
    #[must_use]
    pub fn with_selected_wire<W>(self, wire: W) -> UWireTransport<Self, W>
    where
        W: UWire,
    {
        self.with_wire(wire)
    }

    /// Returns the last prepared metadata request observed by the physical core.
    pub async fn last_prepared(&self) -> Option<PreparedTxLoanSpec> {
        self.state.lock().await.prepared.last().cloned()
    }

    /// Injects one encoded receive frame for pull receive tests.
    pub async fn push_encoded_rx(&self, frame: Iceoryx2EncodedRxFrame) {
        self.state.lock().await.received.push_back(frame);
    }

    /// Delivers one encoded receive frame to registered raw listeners.
    pub async fn deliver_encoded_rx(&self, frame: Iceoryx2EncodedRxFrame) {
        let listeners = self.state.lock().await.listeners.clone();
        for listener in listeners {
            listener.on_receive_encoded_zero_copy(frame.clone()).await;
        }
    }
}

/// Transmit loan returned by the iceoryx2 proof core.
pub struct Iceoryx2TxLoan {
    header: UProtocolHeader,
    encoded_metadata: Vec<u8>,
    buffer: UVecTxBuffer,
}

impl Iceoryx2TxLoan {
    /// Returns selected-wire prepared metadata bytes stored before the payload.
    #[must_use]
    pub fn encoded_metadata(&self) -> &[u8] {
        &self.encoded_metadata
    }

    /// Returns the physical user-header mirror.
    #[must_use]
    pub fn header(&self) -> UProtocolHeader {
        self.header
    }

    fn into_encoded_rx(self) -> Result<Iceoryx2EncodedRxFrame, UStatus> {
        self.header
            .validate_against(self.encoded_metadata.len(), self.buffer.payload().len())?;
        Ok(Iceoryx2EncodedRxFrame {
            header: self.header,
            encoded_metadata: self.encoded_metadata,
            payload: self.buffer.payload().to_vec(),
        })
    }
}

impl UTxBuffer for Iceoryx2TxLoan {
    fn metadata(&self) -> &UFrameMetadata {
        self.buffer.metadata()
    }

    fn payload(&self) -> &[u8] {
        self.buffer.payload()
    }

    fn payload_mut(&mut self) -> &mut [u8] {
        self.buffer.payload_mut()
    }
}

/// Uninitialized transmit loan returned by the iceoryx2 proof core.
pub struct Iceoryx2UninitTxLoan {
    header: UProtocolHeader,
    encoded_metadata: Vec<u8>,
    inner: UVecUninitTxBuffer,
}

impl UUninitTxBuffer for Iceoryx2UninitTxLoan {
    type Initialized = Iceoryx2TxLoan;

    fn metadata(&self) -> &UFrameMetadata {
        self.inner.metadata()
    }

    fn payload_len(&self) -> usize {
        self.inner.payload_len()
    }

    fn payload_uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.inner.payload_uninit_mut()
    }

    unsafe fn assume_payload_init(self) -> Self::Initialized {
        Iceoryx2TxLoan {
            header: self.header,
            encoded_metadata: self.encoded_metadata,
            // SAFETY: the caller of this method guarantees that all visible
            // payload bytes in the uninitialized loan have been initialized.
            buffer: unsafe { self.inner.assume_payload_init() },
        }
    }
}

/// Raw encoded receive frame returned by the physical proof core.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Iceoryx2EncodedRxFrame {
    header: UProtocolHeader,
    encoded_metadata: Vec<u8>,
    payload: Vec<u8>,
}

impl Iceoryx2EncodedRxFrame {
    /// Creates a raw encoded receive frame after mirror validation.
    pub fn new(
        header: UProtocolHeader,
        encoded_metadata: Vec<u8>,
        payload: Vec<u8>,
    ) -> Result<Self, UStatus> {
        header.validate_against(encoded_metadata.len(), payload.len())?;
        Ok(Self {
            header,
            encoded_metadata,
            payload,
        })
    }

    /// Returns the physical user-header mirror.
    #[must_use]
    pub fn header(&self) -> UProtocolHeader {
        self.header
    }
}

impl UEncodedRxFrame for Iceoryx2EncodedRxFrame {
    type PayloadReader<'a>
        = Cursor<&'a [u8]>
    where
        Self: 'a;
    type PayloadSlices<'a>
        = std::iter::Once<&'a [u8]>
    where
        Self: 'a;

    fn encoded_metadata(&self) -> &[u8] {
        &self.encoded_metadata
    }

    fn payload_len(&self) -> usize {
        self.payload.len()
    }

    fn payload_reader(&self) -> Self::PayloadReader<'_> {
        Cursor::new(self.payload.as_slice())
    }

    fn payload_slices(&self) -> Self::PayloadSlices<'_> {
        std::iter::once(self.payload.as_slice())
    }

    fn try_contiguous_payload(&self) -> Option<&[u8]> {
        Some(&self.payload)
    }
}

impl UEncodedLoanedRxFrame for Iceoryx2EncodedRxFrame {
    fn loaned_contiguous_payload(&self) -> Result<LoanedPayload<'_>, up_rust::UWireError> {
        // SAFETY: the slice is borrowed directly from the frame's receive storage;
        // no copy or coalescing is performed to create this view.
        Ok(unsafe {
            LoanedPayload::new_unchecked(&self.payload, PayloadLoanProvenance::OpaqueTransportLoan)
        })
    }
}

#[async_trait]
impl UZeroCopyTransportCore for Iceoryx2WireCore {
    type Tx = Iceoryx2TxLoan;
    type Rx = Iceoryx2EncodedRxFrame;

    async fn loan_prepared_tx(&self, spec: PreparedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let header = UProtocolHeader::from_prepared(&spec)?;
        let encoded_metadata = spec.encoded_metadata().to_vec();
        let buffer = UVecTxBuffer::with_alignment(
            spec.metadata().clone(),
            spec.payload_len(),
            spec.payload_alignment(),
        )?;
        self.state.lock().await.prepared.push(spec);
        Ok(Iceoryx2TxLoan {
            header,
            encoded_metadata,
            buffer,
        })
    }

    async fn send_prepared_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        self.state
            .lock()
            .await
            .received
            .push_back(buffer.into_encoded_rx()?);
        Ok(())
    }

    async fn receive_encoded_zero_copy(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<Self::Rx, UStatus> {
        self.state
            .lock()
            .await
            .received
            .pop_front()
            .ok_or_else(|| UStatus::fail_with_code(UCode::NotFound, "no frame available"))
    }

    async fn register_encoded_zero_copy_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        self.state.lock().await.listeners.push(listener);
        Ok(())
    }

    async fn unregister_encoded_zero_copy_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let mut state = self.state.lock().await;
        let Some(index) = state
            .listeners
            .iter()
            .position(|registered| Arc::ptr_eq(registered, &listener))
        else {
            return Err(UStatus::fail_with_code(
                UCode::NotFound,
                "listener not registered",
            ));
        };
        state.listeners.remove(index);
        Ok(())
    }
}

#[async_trait]
impl UZeroCopyUninitTransportCore for Iceoryx2WireCore {
    type UninitTx = Iceoryx2UninitTxLoan;

    async fn loan_prepared_uninit_tx(
        &self,
        spec: PreparedTxLoanSpec,
    ) -> Result<Self::UninitTx, UStatus> {
        let header = UProtocolHeader::from_prepared(&spec)?;
        let encoded_metadata = spec.encoded_metadata().to_vec();
        self.state.lock().await.prepared.push(spec.clone());
        Ok(Iceoryx2UninitTxLoan {
            header,
            encoded_metadata,
            inner: UVecUninitTxBuffer::with_alignment(
                spec.metadata().clone(),
                spec.payload_len(),
                spec.payload_alignment(),
            )?,
        })
    }
}
