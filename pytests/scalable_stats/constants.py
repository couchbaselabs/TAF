stats_default_config = {
    "retention_size": 1024,
    "retention_time": 365,
    "wal_compression": "false",
    "storage_path": '"./stats_data"',
    "prometheus_auth_enabled": "true",
    "log_level": '"debug"',
    "max_block_duration": 25,
    "scrape_interval": 10,
    "scrape_timeout": 10,
    "snapshot_timeout_msecs": 30000,
    "token_file": "prometheus_token",
    "query_max_samples": 200000,
    "intervals_calculation_period": 600000,
    "cbcollect_stats_dump_max_size": 1073741824,
    "cbcollect_stats_min_period": 14,
    "average_sample_size": 3,
    "services": "[{index,[{high_cardinality_enabled,true}]}, {fts,[{high_cardinality_enabled,true}]}, \
                 {kv,[{high_cardinality_enabled,true}]}, {cbas,[{high_cardinality_enabled,true}]}, \
                 {eventing,[{high_cardinality_enabled,true}]}]",
    "external_prometheus_services": "[{index,[{high_cardinality_enabled,true}]}, \
                                     {fts,[{high_cardinality_enabled,true}]}, \
                                     {kv,[{high_cardinality_enabled,true}]}, \
                                     {cbas,[{high_cardinality_enabled,true}]}, \
                                     {eventing,[{high_cardinality_enabled,true}]}]",
    "prometheus_metrics_enabled": "false",
    "prometheus_metrics_scrape_interval": 60,
    "listen_addr_type": "loopback"
}