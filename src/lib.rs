//! Second-level cache contracts and conservative interception semantics for `RBatis`.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Statement, visit_relations};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;
use tokio::sync::Mutex;

/// Result returned by cache contracts.
pub type CacheResult<T> = Result<T, CacheError>;

/// Stable error model for cache integration boundaries.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CacheError {
    /// SQL could not be classified safely.
    #[error("SQL parsing failed: {0}")]
    Sql(String),
    /// `MessagePack` encoding or decoding failed.
    #[error("cache envelope codec failed: {0}")]
    Codec(String),
    /// A cache backend operation failed.
    #[error("cache backend failed: {0}")]
    Backend(String),
    /// The database loader failed.
    #[error("database loader failed: {0}")]
    Loader(String),
}

/// Parsed statement category used by conservative cache policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    /// Exactly one query statement.
    Select,
    /// A write, DDL, or multi-statement input.
    Other,
}

/// Canonical SQL plus parser-derived relation tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlMetadata {
    /// Stable SQL rendered from the parsed AST.
    pub canonical_sql: String,
    /// Every relation visited by the parser, sorted and deduplicated.
    pub table_tags: BTreeSet<String>,
    /// Conservative statement category.
    pub kind: StatementKind,
}

impl SqlMetadata {
    /// Parses SQL using `sqlparser`; callers must bypass caching on error.
    pub fn parse(sql: &str) -> CacheResult<Self> {
        let statements = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|error| CacheError::Sql(error.to_string()))?;
        let kind = if statements.len() == 1 && matches!(statements[0], Statement::Query(_)) {
            StatementKind::Select
        } else {
            StatementKind::Other
        };
        let mut table_tags = BTreeSet::new();
        let _: ControlFlow<()> = visit_relations(&statements, |relation| {
            table_tags.insert(relation.to_string().to_ascii_lowercase());
            ControlFlow::Continue(())
        });
        Ok(Self {
            canonical_sql: statements
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; "),
            table_tags,
            kind,
        })
    }
}

/// Generation-based invalidation strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationStrategy {
    /// Bump one namespace token after a successful database commit.
    NamespaceGeneration,
    /// Reserve parser-derived table generations for distributed backends.
    TableGeneration,
}

/// Cache admission and storage limits.
#[derive(Debug, Clone)]
pub struct CachePolicy {
    /// Entry time to live.
    pub ttl: Duration,
    /// Maximum encoded database result size.
    pub max_value_size: usize,
    /// Invalidation mode selected by the integration.
    pub invalidation: InvalidationStrategy,
}

impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            ttl: Duration::from_mins(5),
            max_value_size: 1024 * 1024,
            invalidation: InvalidationStrategy::NamespaceGeneration,
        }
    }
}

/// Complete cache identity before hashing.
#[derive(Debug, Clone, Copy)]
pub struct CacheKeyInput<'a> {
    /// Cache protocol version.
    pub version: &'a str,
    /// Logical data source name.
    pub data_source: &'a str,
    /// Database driver identity.
    pub driver: &'a str,
    /// Optional tenant boundary.
    pub tenant: Option<&'a str>,
    /// Mapper or application namespace.
    pub namespace: &'a str,
    /// Stable statement identifier.
    pub statement_id: &'a str,
    /// SQL supplied to the database executor.
    pub sql: &'a str,
    /// Canonical encoded database parameters.
    pub parameters: &'a [u8],
}

/// BLAKE3 cache identity and invalidation metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKey {
    digest: String,
    namespace: String,
    generation: u64,
    table_tags: BTreeSet<String>,
}

impl CacheKey {
    /// Builds a collision-resistant key from every isolation boundary.
    pub fn build(input: CacheKeyInput<'_>, generation: u64) -> CacheResult<Self> {
        let metadata = SqlMetadata::parse(input.sql)?;
        let mut hasher = blake3::Hasher::new();
        for component in [
            input.version,
            input.data_source,
            input.driver,
            input.tenant.unwrap_or("-"),
            input.namespace,
            input.statement_id,
            &metadata.canonical_sql,
        ] {
            update_component(&mut hasher, component.as_bytes());
        }
        update_component(&mut hasher, &generation.to_le_bytes());
        update_component(&mut hasher, input.parameters);
        Ok(Self {
            digest: hasher.finalize().to_hex().to_string(),
            namespace: input.namespace.to_owned(),
            generation,
            table_tags: metadata.table_tags,
        })
    }

    /// Backend-safe digest.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Namespace whose generation participates in the key.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Generation used to construct the key.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Parser-derived relation tags.
    pub const fn table_tags(&self) -> &BTreeSet<String> {
        &self.table_tags
    }
}

/// `MessagePack` value stored by every backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEnvelope {
    /// Cache protocol version.
    pub version: u16,
    /// Namespace generation captured when loaded.
    pub generation: u64,
    /// Unix epoch expiration in milliseconds.
    pub expires_at_ms: u64,
    /// SQL relation tags for diagnostics and future table generations.
    pub table_tags: BTreeSet<String>,
    /// Raw database/encrypted-state result bytes.
    pub payload: Vec<u8>,
}

impl CacheEnvelope {
    /// Creates an envelope using a monotonic cache policy deadline.
    pub fn new(key: &CacheKey, payload: Vec<u8>, ttl: Duration) -> Self {
        Self {
            version: 1,
            generation: key.generation,
            expires_at_ms: now_ms().saturating_add(duration_ms(ttl)),
            table_tags: key.table_tags.clone(),
            payload,
        }
    }

    /// Encodes the canonical `MessagePack` representation.
    pub fn encode(&self) -> CacheResult<Vec<u8>> {
        rmp_serde::to_vec_named(self).map_err(|error| CacheError::Codec(error.to_string()))
    }

    /// Decodes the canonical `MessagePack` representation.
    pub fn decode(bytes: &[u8]) -> CacheResult<Self> {
        rmp_serde::from_slice(bytes).map_err(|error| CacheError::Codec(error.to_string()))
    }

    /// Returns whether the entry is valid for the current generation and time.
    pub fn is_fresh(&self, generation: u64) -> bool {
        self.version == 1 && self.generation == generation && self.expires_at_ms > now_ms()
    }
}

/// Storage SPI implemented by local and distributed cache crates.
pub trait CacheBackend: Send + Sync + 'static {
    /// Gets an encoded envelope.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, CacheResult<Option<Vec<u8>>>>;

    /// Stores an encoded envelope with a backend TTL.
    fn put<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        ttl: Duration,
    ) -> BoxFuture<'a, CacheResult<()>>;

    /// Reads the namespace generation without scanning keys.
    fn generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, CacheResult<u64>>;

    /// Atomically increments the namespace generation.
    fn bump_generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, CacheResult<u64>>;
}

/// Cache interception request; callers pass database/encrypted-state bytes to the loader.
#[derive(Debug, Clone, Copy)]
pub struct CacheRequest<'a> {
    /// Key material.
    pub key: CacheKeyInput<'a>,
    /// Whether the query executes inside a database transaction.
    pub in_transaction: bool,
}

/// Immutable metrics snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheMetricsSnapshot {
    /// Cache hits.
    pub hits: u64,
    /// Cache misses.
    pub misses: u64,
    /// Policy or parse bypasses.
    pub bypasses: u64,
    /// Backend failures hidden by fail-open behavior.
    pub backend_errors: u64,
    /// Database loads performed.
    pub loads: u64,
    /// Successful generation bumps.
    pub invalidations: u64,
}

/// Lock-free counters shared by the interceptor and observability adapters.
#[derive(Debug, Default)]
pub struct CacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    bypasses: AtomicU64,
    backend_errors: AtomicU64,
    loads: AtomicU64,
    invalidations: AtomicU64,
}

impl CacheMetrics {
    /// Returns a consistent-enough operational snapshot.
    pub fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            bypasses: self.bypasses.load(Ordering::Relaxed),
            backend_errors: self.backend_errors.load(Ordering::Relaxed),
            loads: self.loads.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
        }
    }
}

/// Conservative, fail-open cache coordinator with per-key singleflight.
pub struct CacheInterceptor<B> {
    backend: Arc<B>,
    policy: CachePolicy,
    metrics: Arc<CacheMetrics>,
    flights: DashMap<String, Arc<Mutex<()>>>,
}

impl<B> CacheInterceptor<B>
where
    B: CacheBackend,
{
    /// Creates an interceptor over one backend.
    pub fn new(backend: Arc<B>, policy: CachePolicy) -> Self {
        Self {
            backend,
            policy,
            metrics: Arc::new(CacheMetrics::default()),
            flights: DashMap::new(),
        }
    }

    /// Returns shared operational metrics.
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Loads a parsed, transaction-free SELECT through cache; backend errors fail open.
    pub async fn get_or_load<F, Fut>(
        &self,
        request: CacheRequest<'_>,
        loader: F,
    ) -> CacheResult<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = CacheResult<Vec<u8>>>,
    {
        let Ok(metadata) = SqlMetadata::parse(request.key.sql) else {
            return self.bypass(loader).await;
        };
        if request.in_transaction || metadata.kind != StatementKind::Select {
            return self.bypass(loader).await;
        }
        let Ok(generation) = self.backend.generation(request.key.namespace).await else {
            self.metrics.backend_errors.fetch_add(1, Ordering::Relaxed);
            return self.load(loader).await;
        };
        let key = CacheKey::build(request.key, generation)?;
        if let Some(payload) = self.cached(&key).await {
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(payload);
        }
        self.metrics.misses.fetch_add(1, Ordering::Relaxed);
        let flight = self
            .flights
            .entry(key.digest.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let guard = flight.lock().await;
        if let Some(payload) = self.cached(&key).await {
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
            drop(guard);
            self.release_flight(key.digest(), &flight);
            return Ok(payload);
        }
        let payload = self.load(loader).await?;
        if payload.len() <= self.policy.max_value_size {
            let envelope = CacheEnvelope::new(&key, payload.clone(), self.policy.ttl).encode()?;
            if self
                .backend
                .put(key.digest(), envelope, self.policy.ttl)
                .await
                .is_err()
            {
                self.metrics.backend_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        drop(guard);
        self.release_flight(key.digest(), &flight);
        Ok(payload)
    }

    /// Bumps a namespace generation after the caller's database commit succeeds.
    pub async fn invalidate_after_commit(&self, namespace: &str) -> CacheResult<u64> {
        let generation = self.backend.bump_generation(namespace).await?;
        self.metrics.invalidations.fetch_add(1, Ordering::Relaxed);
        Ok(generation)
    }

    async fn cached(&self, key: &CacheKey) -> Option<Vec<u8>> {
        let Ok(bytes) = self.backend.get(key.digest()).await else {
            self.metrics.backend_errors.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let bytes = bytes?;
        CacheEnvelope::decode(&bytes)
            .ok()
            .filter(|envelope| envelope.is_fresh(key.generation))
            .map(|envelope| envelope.payload)
    }

    async fn bypass<F, Fut>(&self, loader: F) -> CacheResult<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = CacheResult<Vec<u8>>>,
    {
        self.metrics.bypasses.fetch_add(1, Ordering::Relaxed);
        self.load(loader).await
    }

    async fn load<F, Fut>(&self, loader: F) -> CacheResult<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = CacheResult<Vec<u8>>>,
    {
        self.metrics.loads.fetch_add(1, Ordering::Relaxed);
        loader().await
    }

    fn release_flight(&self, key: &str, flight: &Arc<Mutex<()>>) {
        if let Entry::Occupied(entry) = self.flights.entry(key.to_owned())
            && Arc::ptr_eq(entry.get(), flight)
            && Arc::strong_count(entry.get()) == 2
        {
            entry.remove();
        }
    }
}

fn update_component(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(bytes);
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}
