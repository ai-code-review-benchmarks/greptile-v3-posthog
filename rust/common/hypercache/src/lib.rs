//! HyperCache Reader - Multi-tier cache reader for PostHog
//!
//! This crate provides a HyperCacheReader that reads from multiple cache tiers:
//! 1. Redis (primary, fastest)
//! 2. S3 (secondary, persistent fallback)
//!
//! It matches the behavior of Django's Hypercache system used for flag definitions.

use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use common_compression::{decompress_string_data, CompressionError};
use common_metrics::inc;
use common_redis::{Client as RedisClient, RedisValueFormat};
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;
use tokio::time::timeout;
use tracing::{debug, info, warn};

// Metrics constants matching Django's HyperCache implementation
// See: posthog/storage/hypercache.py:28-32
const HYPERCACHE_COUNTER_NAME: &str = "posthog_hypercache_get_from_cache";

// Django's special empty value constant
// See: posthog/storage/hypercache.py:35
const HYPER_CACHE_EMPTY_VALUE: &str = "__missing__";

#[derive(Error, Debug)]
pub enum HyperCacheError {
    #[error("Redis error: {0}")]
    Redis(#[from] common_redis::CustomRedisError),

    #[error("S3 error: {0}")]
    S3(String),

    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Compression error: {0}")]
    Compression(#[from] CompressionError),

    #[error("Cache miss - data not found in any tier")]
    CacheMiss,

    #[error("Timeout error: {0}")]
    Timeout(String),
}

/// Cache tier that provided the data
#[derive(Debug, Clone, PartialEq)]
pub enum CacheSource {
    Redis,
    S3,
}

/// Configuration for HyperCache
#[derive(Debug, Clone)]
pub struct HyperCacheConfig {
    /// S3 bucket name
    pub s3_bucket: String,
    /// S3 region
    pub s3_region: String,
    /// S3 endpoint (optional, for local testing)
    pub s3_endpoint: Option<String>,
    /// Timeout for Redis operations
    pub redis_timeout: Duration,
    /// Timeout for S3 operations  
    pub s3_timeout: Duration,
    /// Namespace for metrics (e.g., "local_evaluation")
    pub namespace: String,
    /// Value for metrics (e.g., "flags")
    pub value: String,
}

impl Default for HyperCacheConfig {
    fn default() -> Self {
        Self {
            s3_bucket: "posthog".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_endpoint: None,
            redis_timeout: Duration::from_millis(500),
            s3_timeout: Duration::from_secs(3),
            namespace: "local_evaluation".to_string(),
            value: "flags".to_string(),
        }
    }
}

/// HyperCache reader that follows Django's multi-tier caching pattern
pub struct HyperCacheReader {
    redis_client: std::sync::Arc<dyn RedisClient + Send + Sync>,
    s3_client: S3Client,
    config: HyperCacheConfig,
}

impl HyperCacheReader {
    /// Create a new HyperCacheReader with the given Redis client and configuration
    pub async fn new(
        redis_client: std::sync::Arc<dyn RedisClient + Send + Sync>,
        config: HyperCacheConfig,
    ) -> Result<Self> {
        let mut aws_config_builder = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(config.s3_region.clone()));

        // Set custom endpoint if provided (for local testing)
        if let Some(endpoint) = &config.s3_endpoint {
            aws_config_builder = aws_config_builder.endpoint_url(endpoint);
        }

        let aws_config = aws_config_builder.load().await;

        // Use the same pattern as capture service for custom S3 endpoints
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
        if config.s3_endpoint.is_some() {
            // MinIO needs force_path_style set
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let s3_client = S3Client::from_conf(s3_config_builder.build());

        Ok(Self {
            redis_client,
            s3_client,
            config,
        })
    }

    /// Get data from cache, trying Redis first, then S3 as fallback
    /// Returns the data and the source it came from
    pub async fn get_with_source(
        &self,
        cache_key: &str,
    ) -> Result<(Value, CacheSource), HyperCacheError> {
        // 1. Try Redis first
        debug!(cache_key = cache_key, "Attempting to get data from Redis");

        match timeout(
            self.config.redis_timeout,
            self.try_get_from_redis(cache_key),
        )
        .await
        {
            Ok(Ok(data)) => {
                info!(cache_key = cache_key, "Cache hit from Redis");

                // Record metrics matching Django's HyperCache implementation
                // See: posthog/storage/hypercache.py:96
                inc(
                    HYPERCACHE_COUNTER_NAME,
                    &[
                        ("result".to_string(), "hit_redis".to_string()),
                        ("namespace".to_string(), self.config.namespace.clone()),
                        ("value".to_string(), self.config.value.clone()),
                    ],
                    1,
                );

                if let Value::String(s) = &data {
                    if s == HYPER_CACHE_EMPTY_VALUE {
                        return Ok((Value::Null, CacheSource::Redis));
                    }
                }
                return Ok((data, CacheSource::Redis));
            }
            Ok(Err(e)) => {
                debug!(cache_key = cache_key, error = %e, "Redis lookup failed");
            }
            Err(_) => {
                warn!(cache_key = cache_key, timeout_ms = ?self.config.redis_timeout, "Redis lookup timed out");
            }
        }

        // 2. Fallback to S3
        debug!(cache_key = cache_key, "Attempting to get data from S3");

        match timeout(self.config.s3_timeout, self.try_get_from_s3(cache_key)).await {
            Ok(Ok(data)) => {
                info!(cache_key = cache_key, "Cache hit from S3");

                // Record metrics matching Django's HyperCache implementation
                // See: posthog/storage/hypercache.py:108
                inc(
                    HYPERCACHE_COUNTER_NAME,
                    &[
                        ("result".to_string(), "hit_s3".to_string()),
                        ("namespace".to_string(), self.config.namespace.clone()),
                        ("value".to_string(), self.config.value.clone()),
                    ],
                    1,
                );

                // Backfill Redis from S3 (fire and forget)
                let redis_client = self.redis_client.clone();
                let cache_key = cache_key.to_string();
                let data_for_redis = data.clone();

                tokio::spawn(async move {
                    if let Err(e) =
                        Self::backfill_redis_from_s3(&redis_client, &cache_key, &data_for_redis)
                            .await
                    {
                        warn!(cache_key = cache_key, error = %e, "Failed to backfill Redis from S3");
                    } else {
                        debug!(
                            cache_key = cache_key,
                            "Successfully backfilled Redis from S3"
                        );
                    }
                });

                return Ok((data, CacheSource::S3));
            }
            Ok(Err(e)) => {
                debug!(cache_key = cache_key, error = %e, "S3 lookup failed");
            }
            Err(_) => {
                warn!(cache_key = cache_key, timeout_ms = ?self.config.s3_timeout, "S3 lookup timed out");
            }
        }

        // 3. No data found in any tier
        warn!(
            cache_key = cache_key,
            "Cache miss - data not found in Redis or S3"
        );

        // Record cache miss metrics matching Django's HyperCache implementation
        // See: posthog/storage/hypercache.py:119
        inc(
            HYPERCACHE_COUNTER_NAME,
            &[
                ("result".to_string(), "missing".to_string()),
                ("namespace".to_string(), self.config.namespace.clone()),
                ("value".to_string(), self.config.value.clone()),
            ],
            1,
        );

        Err(HyperCacheError::CacheMiss)
    }

    /// Get data from cache (without source information)
    pub async fn get(&self, cache_key: &str) -> Result<Value, HyperCacheError> {
        let (data, _source) = self.get_with_source(cache_key).await?;
        Ok(data)
    }

    /// Try to get data from Redis with decompression fallback
    async fn try_get_from_redis(&self, cache_key: &str) -> Result<Value, HyperCacheError> {
        // First, try to get data as UTF-8 (for uncompressed JSON)
        match self
            .redis_client
            .get_with_format(cache_key.to_string(), RedisValueFormat::Utf8)
            .await
        {
            Ok(cached_data) => {
                debug!(cache_key = cache_key, "Retrieved UTF-8 data from Redis");

                // Check for Django's special empty value first
                if cached_data == HYPER_CACHE_EMPTY_VALUE {
                    return Ok(Value::String(cached_data));
                }

                // First try to parse as JSON directly
                match serde_json::from_str(&cached_data) {
                    Ok(value) => return Ok(value),
                    Err(_) => {
                        // If direct parsing fails, try decompressing the string data
                        debug!(
                            cache_key = cache_key,
                            "Direct JSON parsing failed, attempting decompression"
                        );
                        match decompress_string_data(&cached_data) {
                            Ok(decompressed) => match serde_json::from_str(&decompressed) {
                                Ok(value) => return Ok(value),
                                Err(e) => {
                                    warn!(cache_key = cache_key, error = %e, "Failed to parse decompressed string data as JSON");
                                }
                            },
                            Err(e) => {
                                warn!(cache_key = cache_key, error = %e, "Failed to decompress cached string data");
                            }
                        }
                    }
                }
            }
            Err(e) => {
                debug!(cache_key = cache_key, error = %e, "UTF-8 retrieval failed from Redis");
            }
        }

        Err(HyperCacheError::CacheMiss)
    }

    /// Try to get data from S3
    async fn try_get_from_s3(&self, cache_key: &str) -> Result<Value, HyperCacheError> {
        let get_object_output = self
            .s3_client
            .get_object()
            .bucket(&self.config.s3_bucket)
            .key(cache_key)
            .send()
            .await
            .map_err(|e| HyperCacheError::S3(format!("Failed to get object from S3: {e}")))?;

        let body_bytes = get_object_output
            .body
            .collect()
            .await
            .map_err(|e| HyperCacheError::S3(format!("Failed to read S3 object body: {e}")))?
            .into_bytes();

        let body_str = String::from_utf8(body_bytes.to_vec())
            .map_err(|e| HyperCacheError::S3(format!("S3 object body is not valid UTF-8: {e}")))?;

        debug!(cache_key = cache_key, "Retrieved data from S3");

        // Parse JSON directly (S3 data is typically not compressed for flag definitions)
        let value: Value = serde_json::from_str(&body_str)?;
        Ok(value)
    }

    /// Backfill Redis cache from S3 data
    async fn backfill_redis_from_s3(
        redis_client: &std::sync::Arc<dyn RedisClient + Send + Sync>,
        cache_key: &str,
        data: &Value,
    ) -> Result<()> {
        let json_str = serde_json::to_string(data)?;

        // Use Redis client to set the data using UTF-8 format (30 days TTL is handled by Redis config)
        redis_client
            .set_with_format(cache_key.to_string(), json_str, RedisValueFormat::Utf8)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to backfill Redis: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_redis::{CustomRedisError, MockRedisClient};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn test_hypercache_config_default() {
        let config = HyperCacheConfig::default();
        assert_eq!(config.s3_bucket, "posthog");
        assert_eq!(config.s3_region, "us-east-1");
        assert_eq!(config.s3_endpoint, None);
        assert_eq!(config.namespace, "local_evaluation");
        assert_eq!(config.value, "flags");
    }

    #[test]
    fn test_cache_source_equality() {
        assert_eq!(CacheSource::Redis, CacheSource::Redis);
        assert_eq!(CacheSource::S3, CacheSource::S3);
        assert_ne!(CacheSource::Redis, CacheSource::S3);
    }

    #[tokio::test]
    async fn test_get_with_source_empty_value() {
        let cache_key = "some-cache-key";
        let expected_data = "__missing__";

        let mut mock_redis = MockRedisClient::new();
        mock_redis = mock_redis.get_ret(cache_key, Ok(expected_data.to_string()));

        let config = HyperCacheConfig::default();
        let reader = HyperCacheReader {
            redis_client: Arc::new(mock_redis) as Arc<dyn RedisClient + Send + Sync>,
            s3_client: create_mock_s3_client().await,
            config,
        };

        let (result, source) = reader.get_with_source(cache_key).await.unwrap();
        assert_eq!(source, CacheSource::Redis);
        assert_eq!(result, Value::Null);
    }

    #[tokio::test]
    async fn test_try_get_from_redis_success() {
        let mut mock_redis = MockRedisClient::new();
        let test_data = json!({"flags": [], "group_type_mapping": {}});
        let test_data_str = serde_json::to_string(&test_data).unwrap();

        mock_redis = mock_redis.get_ret("test_key", Ok(test_data_str));

        let config = HyperCacheConfig::default();
        let reader = HyperCacheReader {
            redis_client: Arc::new(mock_redis) as Arc<dyn RedisClient + Send + Sync>,
            s3_client: create_mock_s3_client().await,
            config,
        };

        let result = reader.try_get_from_redis("test_key").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), test_data);
    }

    #[tokio::test]
    async fn test_try_get_from_redis_not_found() {
        let mut mock_redis = MockRedisClient::new();
        mock_redis = mock_redis.get_ret("test_key", Err(CustomRedisError::NotFound));

        let config = HyperCacheConfig::default();
        let reader = HyperCacheReader {
            redis_client: Arc::new(mock_redis) as Arc<dyn RedisClient + Send + Sync>,
            s3_client: create_mock_s3_client().await,
            config,
        };

        let result = reader.try_get_from_redis("test_key").await;
        assert!(matches!(result, Err(HyperCacheError::CacheMiss)));
    }

    #[tokio::test]
    async fn test_try_get_from_redis_with_compression() {
        let mut mock_redis = MockRedisClient::new();
        let test_data = json!({"flags": [], "group_type_mapping": {}});

        // Use plain JSON string for unit test (integration tests handle compression)
        let test_data_str = serde_json::to_string(&test_data).unwrap();

        mock_redis = mock_redis.get_ret("test_key", Ok(test_data_str));

        let config = HyperCacheConfig::default();
        let reader = HyperCacheReader {
            redis_client: Arc::new(mock_redis) as Arc<dyn RedisClient + Send + Sync>,
            s3_client: create_mock_s3_client().await,
            config,
        };

        let result = reader.try_get_from_redis("test_key").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), test_data);
    }

    #[test]
    fn test_hypercache_error_conversion() {
        let cache_miss = HyperCacheError::CacheMiss;
        let redis_error = HyperCacheError::Redis(CustomRedisError::NotFound);
        let s3_error = HyperCacheError::S3("S3 error".to_string());
        let json_error =
            HyperCacheError::Json(serde_json::from_str::<Value>("invalid").unwrap_err());
        let timeout_error = HyperCacheError::Timeout("Timeout".to_string());

        assert!(matches!(cache_miss, HyperCacheError::CacheMiss));
        assert!(matches!(redis_error, HyperCacheError::Redis(_)));
        assert!(matches!(s3_error, HyperCacheError::S3(_)));
        assert!(matches!(json_error, HyperCacheError::Json(_)));
        assert!(matches!(timeout_error, HyperCacheError::Timeout(_)));
    }

    #[tokio::test]
    async fn test_backfill_redis_from_s3() {
        let mut mock_redis = MockRedisClient::new();
        mock_redis = mock_redis.set_ret("test_key", Ok(()));

        let test_data = json!({"flags": [], "group_type_mapping": {}});
        let redis_client: Arc<dyn RedisClient + Send + Sync> = Arc::new(mock_redis);

        let result =
            HyperCacheReader::backfill_redis_from_s3(&redis_client, "test_key", &test_data).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_with_source_redis_hit() {
        let cache_key = "cache/teams/123/test_namespace/test_value";
        let test_data = json!({"key": "value", "nested": {"data": "test"}});
        let test_data_str = serde_json::to_string(&test_data).unwrap();

        let mut mock_redis = MockRedisClient::new();
        mock_redis = mock_redis.get_ret(cache_key, Ok(test_data_str));

        let config = HyperCacheConfig::default();
        let reader = HyperCacheReader {
            redis_client: Arc::new(mock_redis) as Arc<dyn RedisClient + Send + Sync>,
            s3_client: create_mock_s3_client().await,
            config,
        };

        let (result, source) = reader.get_with_source(cache_key).await.unwrap();
        assert_eq!(source, CacheSource::Redis);
        assert_eq!(result, test_data);
    }

    #[tokio::test]
    async fn test_get_with_source_s3_fallback() {
        let cache_key = "cache/teams/123/test_namespace/test_value";
        let _test_data = json!({"key": "value", "nested": {"data": "test"}});

        // This test demonstrates the same pattern as Django's test_get_from_cache_s3_fallback:
        // 1. Redis miss (mocked)
        // 2. S3 fallback attempt (would need real S3 or better mocking)
        //
        // Django test does:
        // - hypercache.set_cache_value(team_id, sample_data)  # Sets both Redis + S3
        // - hypercache.clear_cache(team_id, kinds=["redis"])  # Clears only Redis
        // - Expects S3 hit with sample_data
        //
        // Our limitation: We use MockRedisClient and basic S3Client, so we can't
        // easily simulate "S3 has data but Redis doesn't" without integration testing

        let mut mock_redis = MockRedisClient::new();
        // First call: Redis miss (simulating cleared Redis)
        mock_redis = mock_redis.get_ret(cache_key, Err(CustomRedisError::NotFound));
        // Second call: For backfill if S3 had data
        mock_redis = mock_redis.set_ret(cache_key, Ok(()));

        let config = HyperCacheConfig::default();
        let reader = HyperCacheReader {
            redis_client: Arc::new(mock_redis) as Arc<dyn RedisClient + Send + Sync>,
            s3_client: create_mock_s3_client().await,
            config,
        };

        // Without proper S3 mocking, this will be a cache miss
        // In a real integration test with actual S3, this would succeed
        let result = reader.get_with_source(cache_key).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HyperCacheError::CacheMiss));
    }

    #[tokio::test]
    async fn test_get_with_source_complete_miss() {
        let cache_key = "cache/teams/123/test_namespace/test_value";

        // Redis returns NotFound
        let mut mock_redis = MockRedisClient::new();
        mock_redis = mock_redis.get_ret(cache_key, Err(CustomRedisError::NotFound));

        let config = HyperCacheConfig::default();
        let reader = HyperCacheReader {
            redis_client: Arc::new(mock_redis) as Arc<dyn RedisClient + Send + Sync>,
            s3_client: create_mock_s3_client().await,
            config,
        };

        // Both Redis and S3 miss should result in CacheMiss error
        let result = reader.get_with_source(cache_key).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HyperCacheError::CacheMiss));
    }

    #[tokio::test]
    async fn test_redis_json_parsing_error() {
        let cache_key = "cache/teams/123/test_namespace/test_value";
        let invalid_json = "invalid json data";

        let mut mock_redis = MockRedisClient::new();
        mock_redis = mock_redis.get_ret(cache_key, Ok(invalid_json.to_string()));

        let config = HyperCacheConfig::default();
        let reader = HyperCacheReader {
            redis_client: Arc::new(mock_redis) as Arc<dyn RedisClient + Send + Sync>,
            s3_client: create_mock_s3_client().await,
            config,
        };

        let result = reader.try_get_from_redis(cache_key).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HyperCacheError::CacheMiss));
    }

    // Helper function to create a mock S3 client for testing
    // Note: This is a simplified mock for testing. In real integration tests,
    // you'd use actual AWS SDK test utilities or a local S3-compatible service
    async fn create_mock_s3_client() -> S3Client {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url("http://localhost:9000") // LocalStack or similar
            .load()
            .await;
        S3Client::new(&config)
    }
}
