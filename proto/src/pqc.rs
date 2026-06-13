//! Application-layer **post-quantum wire wrap** (CRYSTALS-Kyber-768 KEM), layered on top of
//! the TLS 1.3 tunnel for defence in depth (the double-layer model in ARCHITECTURE.md).
//!
//! Each message payload is sealed with the recipient's Kyber public key: the KEM encapsulates a
//! fresh shared secret that keys an AEAD over the serialized payload. Even if the (classical) TLS
//! layer were broken by a future quantum adversary, the inner payload stays confidential.
//!
//! Two ways to use it:
//! - [`seal`] / [`open`] — wrap an arbitrary byte payload (e.g. a sensitive proto field) into an
//!   opaque envelope. This is the same KEM the custodian already uses for ticket bodies.
//! - [`KyberCodec`] — a drop-in `tonic::codec::Codec` that transparently seals/opens **whole gRPC
//!   message bodies**, so a service can opt a method into PQ wire encryption via the low-level
//!   `tonic::client::Grpc` / `tonic::server::Grpc` APIs.
//!
//! Opt-in via env (`PQC_PUBLIC_KEY` / `PQC_PRIVATE_KEY` PEM/raw key files); plaintext-payload
//! default for dev/test.

use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Context, Result};
use prost::Message;
use prost::bytes::{Buf, BufMut};
use shared::encryption::{EncryptedData, EncryptionService};
use tonic::Status;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

/// Path to this service's Kyber public key (peers seal to it).
pub const ENV_PUB: &str = "PQC_PUBLIC_KEY";
/// Path to this service's Kyber private key (used to open inbound payloads).
pub const ENV_PRIV: &str = "PQC_PRIVATE_KEY";

/// Whether the PQ wire wrap is configured (both key paths set).
#[must_use]
pub fn enabled() -> bool {
    std::env::var(ENV_PUB).is_ok() && std::env::var(ENV_PRIV).is_ok()
}

/// Seal `plaintext` to `peer_public_key` (Kyber-768 KEM + AEAD). Returns an opaque envelope.
///
/// # Errors
///
/// Returns an error if the public key is malformed or encryption fails.
pub fn seal(plaintext: &[u8], peer_public_key: &[u8]) -> Result<Vec<u8>> {
    let envelope = EncryptionService::encrypt_with_public_key(plaintext, peer_public_key)
        .map_err(|e| anyhow::anyhow!("pqc seal failed: {e}"))?;
    serde_json::to_vec(&envelope).context("serialize pqc envelope")
}

/// Open a sealed envelope with our Kyber private key, recovering the plaintext.
///
/// # Errors
///
/// Returns an error if the envelope is malformed or decryption fails.
pub fn open(envelope: &[u8], private_key: &[u8]) -> Result<Vec<u8>> {
    let envelope: EncryptedData =
        serde_json::from_slice(envelope).context("deserialize pqc envelope")?;
    EncryptionService::decrypt_with_private_key(&envelope, private_key)
        .map_err(|e| anyhow::anyhow!("pqc open failed: {e}"))
}

/// A `tonic::codec::Codec` that Kyber-seals outbound message bodies and opens inbound ones.
///
/// `seal_to` is the peer's Kyber public key (used to seal what we **encode**); `open_with` is our
/// own Kyber private key (used to open what we **decode**). On a client these are the server's
/// public key + the client's private key; on a server they are the client's public key + the
/// server's private key.
pub struct KyberCodec<E, D> {
    seal_to: Arc<Vec<u8>>,
    open_with: Arc<Vec<u8>>,
    _marker: PhantomData<(E, D)>,
}

impl<E, D> KyberCodec<E, D> {
    #[must_use]
    pub fn new(seal_to: Vec<u8>, open_with: Vec<u8>) -> Self {
        Self {
            seal_to: Arc::new(seal_to),
            open_with: Arc::new(open_with),
            _marker: PhantomData,
        }
    }
}

pub struct KyberEncoder<E> {
    seal_to: Arc<Vec<u8>>,
    _marker: PhantomData<E>,
}

pub struct KyberDecoder<D> {
    open_with: Arc<Vec<u8>>,
    _marker: PhantomData<D>,
}

impl<E: Message> Encoder for KyberEncoder<E> {
    type Item = E;
    type Error = Status;

    fn encode(&mut self, item: E, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        let plaintext = item.encode_to_vec();
        let sealed = seal(&plaintext, &self.seal_to)
            .map_err(|e| Status::internal(format!("pqc seal: {e}")))?;
        dst.put_slice(&sealed);
        Ok(())
    }
}

impl<D: Message + Default> Decoder for KyberDecoder<D> {
    type Item = D;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<D>, Status> {
        if !src.has_remaining() {
            return Ok(None);
        }
        let mut sealed = vec![0u8; src.remaining()];
        src.copy_to_slice(&mut sealed);
        let plaintext = open(&sealed, &self.open_with)
            .map_err(|e| Status::internal(format!("pqc open: {e}")))?;
        let message = D::decode(plaintext.as_slice())
            .map_err(|e| Status::internal(format!("decode: {e}")))?;
        Ok(Some(message))
    }
}

impl<E: Message + Send + 'static, D: Message + Default + Send + 'static> Codec
    for KyberCodec<E, D>
{
    type Encode = E;
    type Decode = D;
    type Encoder = KyberEncoder<E>;
    type Decoder = KyberDecoder<D>;

    fn encoder(&mut self) -> Self::Encoder {
        KyberEncoder {
            seal_to: self.seal_to.clone(),
            _marker: PhantomData,
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        KyberDecoder {
            open_with: self.open_with.clone(),
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_open_round_trips() {
        let (pk, sk) = EncryptionService::generate_keypair().unwrap();
        let plaintext = b"sensitive payload over the wire";
        let envelope = seal(plaintext, &pk).unwrap();

        // The envelope is opaque ciphertext, not the plaintext.
        assert_ne!(envelope.as_slice(), plaintext.as_slice());
        assert!(!envelope.windows(plaintext.len()).any(|w| w == plaintext));

        let recovered = open(&envelope, &sk).unwrap();
        assert_eq!(recovered, plaintext);
    }

    #[test]
    fn open_with_wrong_key_fails() {
        let (pk, _sk) = EncryptionService::generate_keypair().unwrap();
        let (_pk2, sk2) = EncryptionService::generate_keypair().unwrap();
        let envelope = seal(b"secret", &pk).unwrap();
        assert!(open(&envelope, &sk2).is_err());
    }

    #[test]
    fn seal_rejects_malformed_public_key() {
        assert!(seal(b"x", &[0u8; 8]).is_err());
    }

    #[test]
    fn enabled_is_false_without_env() {
        // The test environment sets neither PQC_PUBLIC_KEY nor PQC_PRIVATE_KEY.
        assert!(!enabled());
    }

    #[test]
    fn codec_constructs_encoder_and_decoder() {
        let (pk, sk) = EncryptionService::generate_keypair().unwrap();
        let mut codec: KyberCodec<crate::admin::IntrusionAck, crate::admin::IntrusionAck> =
            KyberCodec::new(pk, sk);
        // Exercise the Codec plumbing (encoder/decoder construction + Arc key sharing).
        let _enc = codec.encoder();
        let _dec = codec.decoder();
    }

    #[test]
    fn codec_pipeline_round_trips_a_prost_message() {
        // Exercises exactly what `KyberEncoder::encode` + `KyberDecoder::decode` do:
        // prost-encode -> Kyber-seal -> (wire) -> Kyber-open -> prost-decode.
        let (server_pk, server_sk) = EncryptionService::generate_keypair().unwrap();
        let message = crate::admin::IntrusionAck { recorded: true };

        let sealed = seal(&message.encode_to_vec(), &server_pk).unwrap();
        assert_ne!(sealed, message.encode_to_vec());

        let plaintext = open(&sealed, &server_sk).unwrap();
        let decoded = crate::admin::IntrusionAck::decode(plaintext.as_slice()).unwrap();
        assert_eq!(decoded, message);
    }
}
