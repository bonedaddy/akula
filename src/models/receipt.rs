use super::*;
use crate::crypto::*;
use bytes::{BufMut, Bytes, BytesMut};
use fastrlp::*;
use serde::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Receipt {
    pub tx_type: TxType,
    pub success: bool,
    pub cumulative_gas_used: u64,
    pub bloom: Bloom,
    pub logs: Vec<Log>,
}

impl Receipt {
    pub fn new(tx_type: TxType, success: bool, cumulative_gas_used: u64, logs: Vec<Log>) -> Self {
        let bloom = logs_bloom(&logs);
        Self {
            tx_type,
            success,
            cumulative_gas_used,
            bloom,
            logs,
        }
    }

    fn decode_inner(rlp: &rlp::Rlp, is_legacy: bool) -> Result<Self, DecoderError> {
        Ok(match is_legacy {
            true => {
                let inner = UntypedReceipt::decode(rlp)?;
                inner.into_receipt(TxType::Legacy)
            }
            false => {
                let tx_type = u8::decode(rlp)?;
                let inner = UntypedReceipt::decode(rlp)?;
                inner.into_receipt(tx_type.try_into()?)
            }
        })
    }

    fn rlp_header(&self) -> fastrlp::Header {
        let h = fastrlp::Header {
            list: true,
            payload_length: 0,
        };

        h.payload_length += Encodable::length(&self.success);
        h.payload_length += Encodable::length(&self.cumulative_gas_used);
        h.payload_length += Encodable::length(&self.bloom);
        h.payload_length += Encodable::length(&self.logs);

        h
    }

    fn encode_inner(&self, out: &mut dyn BufMut, rlp_head: Header) {
        if !matches!(self.tx_type, TxType::Legacy) {
            out.put_u8(self.tx_type as u8);
        }

        rlp_head.encode(out);
        Encodable::encode(&self.success, out);
        Encodable::encode(&self.cumulative_gas_used, out);
        Encodable::encode(&self.bloom, out);
        Encodable::encode(&self.logs, out);
    }
}

impl Encodable for Receipt {
    fn length(&self) -> usize {
        let rlp_head = self.rlp_header();
        let rlp_len = length_of_length(rlp_head.payload_length) + rlp_head.payload_length;
        if matches!(self.tx_type, TxType::Legacy) {
            rlp_len
        } else {
            // EIP-2718 objects are wrapped into byte array in containing RLP
            length_of_length(rlp_len + 1) + rlp_len + 1
        }
    }

    fn encode(&self, out: &mut dyn BufMut) {
        let rlp_head = self.rlp_header();

        if !matches!(self.tx_type, TxType::Legacy) {
            let rlp_len = length_of_length(rlp_head.payload_length) + rlp_head.payload_length;
            Header {
                list: false,
                payload_length: rlp_len + 1,
            }
            .encode(out);
        }

        out.put_u8(self.tx_type as u8);

        self.encode_inner(out, rlp_head);
    }
}

impl TrieEncode for Receipt {
    fn trie_encode(&self) -> Bytes {
        let mut s = BytesMut::new();
        self.encode_inner(&mut s, self.rlp_header());
        s.freeze()
    }
}

impl Decodable for Receipt {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let rlp_head = Header::decode(buf)?;

        Ok(if rlp_head.list {
            let started_len = buf.len();
            let tx = legacy_decode(buf)?;

            let consumed = started_len - buf.len();
            if consumed != rlp_head.payload_length {
                return Err(fastrlp::DecodeError::ListLengthMismatch {
                    expected: rlp_head.payload_length,
                    got: consumed,
                });
            }

            tx
        } else if rlp_head.payload_length == 0 {
            return Err(DecodeError::InputTooShort);
        } else {
            if buf.is_empty() {
                return Err(DecodeError::InputTooShort);
            }

            let tx_type = TxType::from_u8(buf[0])?;
        })
    }
}

#[derive(RlpDecodable)]
struct UntypedReceipt {
    pub success: bool,
    pub cumulative_gas_used: u64,
    pub bloom: Bloom,
    pub logs: Vec<Log>,
}

impl UntypedReceipt {
    fn into_receipt(self, tx_type: TxType) -> Receipt {
        Receipt {
            tx_type,
            success: self.success,
            cumulative_gas_used: self.cumulative_gas_used,
            bloom: self.bloom,
            logs: self.logs,
        }
    }
}
