
{
    "GleamBookUsers0" : {
        "key_prefix": "test_docs-",
        "key_size": 32,
        "key_type": "SimpleKey",
        "valType": "MSMARCOEmbeddingProduct",
        "vecFilePath": "/mnt/nfsdata/collection.vecs",
        "scopes": 1,
        "collections": 5,
        # Capped at DocLoader's MSMARCO STEPS upper bound (8,841,823).
        "num_items":  8841823,
        "ops": 50000,
        "rebl_ops_rate": 1000,
        "doc_size": 1024,
        "doc_op_percentages": {"create": 0, "update": 100, "delete": 0, "read": 0, "expiry": 0},
        "2iQPS": 10,
        # DocLoader workers per step range. One MSMARCO worker holds ~9.5k/s,
        # just under a collection's 10,000/s, so 2 workers (5,000/s each) give
        # the full rate; 200/s mutations need 1. See "Process concurrency" in
        # pytests/aGoodDoctor/AGENTS.md.
        "process_concurrency": 2,
        "rebl_process_concurrency": 1,
        # Each collection: the same 4 BHIVE sparse vector indexes + 1 scalar
        # index; query i pairs with index i.
        "collections_defn": [
            {
                "collection_id": "GB0_C01",
                "num_items": 8841823,
                "valType": "MSMARCOEmbeddingProduct",
                "dim": 384,
                "vector": [
                    {
                        "similarity": "DOT",
                        "nProbe": 20,
                        "description": "IVF1024"
                    }
                ],
                "indexes": [
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(5)],
                "2i": {"num_queries": 5,
                       "num_indexes": 5}
            },
            {
                "collection_id": "GB0_C02",
                "num_items": 8841823,
                "valType": "MSMARCOEmbeddingProduct",
                "dim": 384,
                "vector": [
                    {
                        "similarity": "DOT",
                        "nProbe": 20,
                        "description": "IVF1024"
                    }
                ],
                "indexes": [
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(5)],
                "2i": {"num_queries": 5,
                       "num_indexes": 5}
            },
            {
                "collection_id": "GB0_C03",
                "num_items": 8841823,
                "valType": "MSMARCOEmbeddingProduct",
                "dim": 384,
                "vector": [
                    {
                        "similarity": "DOT",
                        "nProbe": 20,
                        "description": "IVF1024"
                    }
                ],
                "indexes": [
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(5)],
                "2i": {"num_queries": 5,
                       "num_indexes": 5}
            },
            {
                "collection_id": "GB0_C04",
                "num_items": 8841823,
                "valType": "MSMARCOEmbeddingProduct",
                "dim": 384,
                "vector": [
                    {
                        "similarity": "DOT",
                        "nProbe": 20,
                        "description": "IVF1024"
                    }
                ],
                "indexes": [
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(5)],
                "2i": {"num_queries": 5,
                       "num_indexes": 5}
            },
            {
                "collection_id": "GB0_C05",
                "num_items": 8841823,
                "valType": "MSMARCOEmbeddingProduct",
                "dim": 384,
                "vector": [
                    {
                        "similarity": "DOT",
                        "nProbe": 20,
                        "description": "IVF1024"
                    }
                ],
                "indexes": [
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(5)],
                "2i": {"num_queries": 5,
                       "num_indexes": 5}
            }
        ]
    }
}
