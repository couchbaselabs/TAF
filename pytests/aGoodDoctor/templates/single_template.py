
{
    "GleamBookUsers0" : {
        "key_prefix": "test_docs-",
        "key_size": 32,
        "key_type": "SimpleKey",
        "valType": "MSMARCOEmbeddingProduct",
        "vecFilePath": "/mnt/nfsdata/collection.vecs",
        "scopes": 1,
        "collections": 1,
        # Capped at DocLoader's MSMARCO STEPS upper bound (8,841,823).
        "num_items":  8841823,
        "ops": 50000,
        "rebl_ops_rate": 1000,
        "doc_size": 1024,
        "doc_op_percentages": {"create": 0, "update": 100, "delete": 0, "read": 0, "expiry": 0},
        "2iQPS": 10,
        "process_concurrency": 10,
        "collections_defn": [
            {
                "collection_id": "8M",
                "num_items": 8841823,
                "valType": "MSMARCOEmbeddingProduct",
                "dim": 384,
                "vector": [
                    {
                        "similarity": "DOT",
                        "quantization": "SQ8",
                        "nProbe": 20,
                        "description": "IVF1024"
                    }
                ],
                "indexes": [
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(7)],
                "2i": {"num_queries": 7,
                       "num_indexes": 7}
            }
        ]
    },

    "GleamBookUsers1" : {
        "key_prefix": "test_docs-",
        "key_size": 32,
        "key_type": "SimpleKey",
        "valType": "MSMARCOEmbeddingProduct",
        "vecFilePath": "/mnt/nfsdata/collection.vecs",
        "scopes": 1,
        "collections": 1,
        # Capped at DocLoader's MSMARCO STEPS upper bound (8,841,823).
        "num_items":  8841823,
        "ops": 50000,
        "rebl_ops_rate": 1000,
        "doc_size": 1024,
        "doc_op_percentages": {"create": 0, "update": 100, "delete": 0, "read": 0, "expiry": 0},
        "2iQPS": 10,
        "ftsQPS": 0,
        "cbasQPS": 0,
        "process_concurrency": 10,
        "collections_defn": [
            {
                "collection_id": "8M",
                "num_items": 8841823,
                "valType": "MSMARCOEmbeddingProduct",
                "dim": 384,
                "vector": [
                    {
                        "similarity": "DOT",
                        "quantization": "SQ8",
                        "nProbe": 20,
                        "description": "IVF1024"
                    }
                ],
                "indexes": [
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(7)],
                "2i": {"num_queries": 7,
                       "num_indexes": 7}
            }
        ]
    },

    "GleamBookUsers2" : {
        "key_prefix": "test_docs-",
        "key_size": 32,
        "key_type": "SimpleKey",
        "valType": "siftBigANN",
        "baseFilePath": "/mnt/nfsdata/bigann",
        "scopes": 1,
        "collections": 1,
        "ops":  50000,
        "rebl_ops_rate": 1000,
        "doc_size": 1024,
        "doc_op_percentages": {"create": 0, "update": 100, "delete": 0, "read": 0, "expiry": 0},
        "2iQPS": 50,
        "process_concurrency": 10,
        "collections_defn" : [
            {
                "collection_id": "1M",
                "num_items": 1000000,
                "valType": "siftBigANN",
                "dim": 128,
                "vector": [
                    {
                        "similarity": "L2_SQUARED",
                        "quantization": "SQ8",
                        "nProbe": 20
                    }
                ],
                "indexes": [
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
                "queries": [
                    'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                    'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                    'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                    'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
                "2i": {"num_queries": 4,
                       "num_indexes": 4},
            },
        ]
    }
}