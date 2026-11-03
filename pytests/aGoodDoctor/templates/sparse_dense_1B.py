
{
    "GleamBookUsers0" : {
        "key_prefix": "test_docs-",
        "key_size": 32,
        "key_type": "SimpleKey",
        "valType": "MSMARCOEmbeddingProduct",
        "vecFilePath": "/mnt/nfsdata/collection.vecs",
        "scopes": 1,
        "collections": 20,
        # Capped at DocLoader's MSMARCO STEPS upper bound (8,841,823).
        "num_items":  8841823,
        "ops": 50000,
        "rebl_ops_rate": 1000,
        "doc_size": 1024,
        "doc_op_percentages": {"create": 0, "update": 100, "delete": 0, "read": 0, "expiry": 0},
        "2iQPS": 100,
        # DocLoader workers per step range. 1 runs each collection at its full
        # rate (one MSMARCO worker holds up to ~9.5k/s) on the fewest threads.
        # See "Process concurrency" in pytests/aGoodDoctor/AGENTS.md.
        "process_concurrency": 1,
        "rebl_process_concurrency": 1,
        "collections_defn": [
            {
                "collection_id": "GB0_C01",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C02",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C03",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C04",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C05",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C06",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C07",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C08",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C09",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C10",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C11",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C12",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C13",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C14",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C15",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C16",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C17",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C18",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C19",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
            },
            {
                "collection_id": "GB0_C20",
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
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) INCLUDE (size, country) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(3)],
                "2i": {"num_queries": 3,
                       "num_indexes": 3}
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
        "collections": 30,
        # Capped at DocLoader's MSMARCO STEPS upper bound (8,841,823).
        "num_items":  8841823,
        "ops": 50000,
        "rebl_ops_rate": 1000,
        "doc_size": 1024,
        "doc_op_percentages": {"create": 0, "update": 100, "delete": 0, "read": 0, "expiry": 0},
        "2iQPS": 100,
        "ftsQPS": 0,
        "cbasQPS": 0,
        # DocLoader workers per step range. 1 runs each collection at its full
        # rate (one MSMARCO worker holds up to ~9.5k/s) on the fewest threads.
        # See "Process concurrency" in pytests/aGoodDoctor/AGENTS.md.
        "process_concurrency": 1,
        "rebl_process_concurrency": 1,
        "collections_defn": [
            {
                "collection_id": "GB1_C01",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C02",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C03",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C04",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C05",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C06",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C07",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C08",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C09",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C10",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C11",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C12",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C13",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C14",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C15",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C16",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C17",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C18",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C19",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C20",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C21",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C22",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C23",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C24",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C25",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C26",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C27",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C28",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C29",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, `sparse` SPARSE VECTOR) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100',
                    'SELECT meta().id FROM {bucket}.{scope}.{collection} WHERE size = 5 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
            },
            {
                "collection_id": "GB1_C30",
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
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(color, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(color) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, size, `sparse` SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "similarity": "DOT", "description": "IVF1024", "scan_nprobes": 20}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(size, color, brand) WITH {{"defer_build":true}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, type, review) WITH {{"defer_build":true}};'
                ],
                "queries": [
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT meta().id, SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}), country FROM {bucket}.{scope}.{collection} WHERE size < 6 AND country = "USA" ORDER BY SPARSE_VECTOR_DISTANCE(`sparse`, $vector, {nProbe}) LIMIT 10',
                    'SELECT brand, color FROM {bucket}.{scope}.{collection} WHERE size = 5 AND color = "Green" LIMIT 100',
                    'SELECT country, type FROM {bucket}.{scope}.{collection} WHERE country = "USA" AND review >= 1 LIMIT 100'
                ],
                "queryParams": [{} for _ in range(4)],
                "2i": {"num_queries": 4,
                       "num_indexes": 4}
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
        "2iQPS": 100,
        # DocLoader workers per step range. 4 x 12,500/s is the fewest that
        # reach the full 50000/s (one worker tops out near 27.8k/s, two near
        # 2 x 22.4k/s) and it divides every step range, so no doc is skipped.
        # Mutations at 1000/s need 1. See "Process concurrency" in
        # pytests/aGoodDoctor/AGENTS.md.
        "process_concurrency": 4,
        "rebl_process_concurrency": 1,
        "collections_defn" : [
            {
                "collection_id": "500M",
                "num_items": 500000000,
                "valType": "siftBigANN",
                "dim": 128,
                # Index i takes vector entry i (this is the bucket's only
                # collection), so the composite indexes get SQ8, RaBitQ and
                # PQ128x8 and the vector indexes SQ8 and RaBitQ.
                "vector": [
                    {
                        "similarity":"L2_SQUARED",
                        "quantization": "SQ8",
                        "nProbe": 20
                    },
                    {
                        "similarity":"L2_SQUARED",
                        "quantization": "SQ8",
                        "nProbe": 20
                    },
                    {
                        "similarity":"L2_SQUARED",
                        "quantization": "RaBitQ",
                        "nProbe": 20
                    },
                    {
                        "similarity":"L2_SQUARED",
                        "quantization": "RaBitQ",
                        "nProbe": 20
                    },
                    {
                        "similarity":"L2_SQUARED",
                        "quantization": "PQ128x8",
                        "nProbe": 20
                    }
                ],
                "indexes": [
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                    'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                    'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
                "queries": [
                    'SELECT id from {bucket}.{scope}.{collection} where size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                    'SELECT id from {bucket}.{scope}.{collection} where color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                    'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                    'SELECT id from {bucket}.{scope}.{collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                    'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
                "2i": {"num_queries": 5,
                       "num_indexes": 5},
            },
        ]
    }
}