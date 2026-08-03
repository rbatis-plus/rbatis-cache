//! 压缩 transcoder。
//!
//! 对应 Java：`org.mybatis.caches.memcached.CompressorTranscoder`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/CompressorTranscoder.java`）。
//!
//! Java 侧在序列化/反序列化时调用 GZIP 压缩；本 crate 用 `flate2::write::GzEncoder`
//! 实现等价语义。

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};

/// 压缩 transcoder。
///
/// 对应 `CompressorTranscoder#encode/decode`。
pub struct CompressorTranscoder;

impl CompressorTranscoder {
    /// GZIP 压缩。
    pub fn encode(value: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(value)?;
        encoder.finish()
    }

    /// GZIP 解压。
    pub fn decode(value: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut decoder = GzDecoder::new(value);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out)?;
        Ok(out)
    }
}
