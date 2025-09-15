use common_compression::encode_base64;
use common_hypercache::{CacheSource, HyperCacheConfig, HyperCacheReader};
use common_redis::{Client as RedisClientTrait, RedisClient, RedisValueFormat};
use serde_json::Value;
use std::env;
use tokio::time::{sleep, Duration};

struct TestClients {
    hypercache: HyperCacheReader,
    redis_client: RedisClient,
    s3_client: aws_sdk_s3::Client,
    s3_bucket: String,
}

async fn setup_integration_clients() -> anyhow::Result<TestClients> {
    // Set AWS credentials for MinIO
    env::set_var("AWS_ACCESS_KEY_ID", "object_storage_root_user");
    env::set_var("AWS_SECRET_ACCESS_KEY", "object_storage_root_password");

    let config = HyperCacheConfig {
        s3_endpoint: Some("http://localhost:19000".to_string()),
        s3_bucket: "posthog".to_string(), // Use the bucket that MinIO creates
        s3_region: "us-east-1".to_string(),
        namespace: "integration_test".to_string(),
        value: "flags".to_string(),
        ..Default::default()
    };

    // Connect to real Redis instance running in CI
    let redis_client = RedisClient::new("redis://localhost:6379".to_string()).await?;

    // Create S3 client with same config (following capture/src/sinks/s3.rs pattern)
    let mut aws_config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(config.s3_region.clone()));

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

    let s3_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    // Create separate Redis client instances for HyperCacheReader and test helpers
    let redis_client_for_cache = RedisClient::new("redis://localhost:6379".to_string()).await?;
    let hypercache =
        HyperCacheReader::new(std::sync::Arc::new(redis_client_for_cache), config.clone()).await?;

    Ok(TestClients {
        hypercache,
        redis_client,
        s3_client,
        s3_bucket: config.s3_bucket,
    })
}

async fn wait_for_services() -> anyhow::Result<()> {
    // Wait a bit for services to be ready
    sleep(Duration::from_millis(100)).await;
    Ok(())
}

/// Helper to set cache data in both Redis and S3 (mimics Django's set_cache_value)
/// This would normally be a method on HyperCacheReader, but for testing we implement it here
async fn set_cache_value(
    redis_client: &RedisClient,
    s3_client: &aws_sdk_s3::Client,
    s3_bucket: &str,
    cache_key: &str,
    data: &Value,
) -> anyhow::Result<()> {
    let json_str = serde_json::to_string(data)?;

    // Redis stores compressed/encoded data
    let compressed_data = encode_base64(json_str.as_bytes());
    redis_client
        .set_with_format(
            cache_key.to_string(),
            compressed_data,
            RedisValueFormat::Utf8,
        )
        .await?;

    // S3 stores plain JSON (as HyperCacheReader expects)
    s3_client
        .put_object()
        .bucket(s3_bucket)
        .key(cache_key)
        .body(json_str.into_bytes().into())
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to set S3 object: {}", e))?;

    Ok(())
}

/// Helper to clear cache from specific tiers (mimics Django's clear_cache)
async fn clear_cache(
    redis_client: &RedisClient,
    s3_client: &aws_sdk_s3::Client,
    s3_bucket: &str,
    cache_key: &str,
    kinds: Option<&[&str]>,
) -> anyhow::Result<()> {
    let kinds = kinds.unwrap_or(&["redis", "s3"]);

    if kinds.contains(&"redis") {
        if let Err(e) = redis_client.del(cache_key.to_string()).await {
            // Don't fail if key doesn't exist
            tracing::debug!("Redis delete failed (key may not exist): {}", e);
        }
    }

    if kinds.contains(&"s3") {
        if let Err(e) = s3_client
            .delete_object()
            .bucket(s3_bucket)
            .key(cache_key)
            .send()
            .await
        {
            // Don't fail if key doesn't exist
            tracing::debug!("S3 delete failed (key may not exist): {}", e);
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_hypercache_redis_s3_fallback() -> anyhow::Result<()> {
    wait_for_services().await?;

    let clients = setup_integration_clients().await?;
    let cache_key = "test-fallback-key";
    let test_data = serde_json::json!({"message": "integration test data"});

    // 1. Set cache value (should go to both Redis and S3)
    set_cache_value(
        &clients.redis_client,
        &clients.s3_client,
        &clients.s3_bucket,
        cache_key,
        &test_data,
    )
    .await?;

    // 2. Clear Redis cache to force S3 fallback
    clear_cache(
        &clients.redis_client,
        &clients.s3_client,
        &clients.s3_bucket,
        cache_key,
        Some(&["redis"]),
    )
    .await?;

    // 3. Get value - should fallback to S3
    let (result, source) = clients.hypercache.get_with_source(cache_key).await?;

    assert_eq!(result, test_data);
    assert_eq!(source, CacheSource::S3);

    // Clean up
    clear_cache(
        &clients.redis_client,
        &clients.s3_client,
        &clients.s3_bucket,
        cache_key,
        None,
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_hypercache_missing_value() -> anyhow::Result<()> {
    wait_for_services().await?;

    let clients = setup_integration_clients().await?;
    let cache_key = "test-missing-key";

    // Clear any existing value
    clear_cache(
        &clients.redis_client,
        &clients.s3_client,
        &clients.s3_bucket,
        cache_key,
        None,
    )
    .await?;

    // Should return a cache miss error
    let result = clients.hypercache.get_with_source(cache_key).await;

    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_hypercache_redis_hit() -> anyhow::Result<()> {
    wait_for_services().await?;

    let clients = setup_integration_clients().await?;
    let cache_key = "test-redis-hit-key";
    let test_data = serde_json::json!({"message": "redis test data"});

    // Set cache value
    set_cache_value(
        &clients.redis_client,
        &clients.s3_client,
        &clients.s3_bucket,
        cache_key,
        &test_data,
    )
    .await?;

    // Get value - should hit Redis
    let (result, source) = clients.hypercache.get_with_source(cache_key).await?;

    assert_eq!(result, test_data);
    assert_eq!(source, CacheSource::Redis);

    // Clean up
    clear_cache(
        &clients.redis_client,
        &clients.s3_client,
        &clients.s3_bucket,
        cache_key,
        None,
    )
    .await?;

    Ok(())
}
