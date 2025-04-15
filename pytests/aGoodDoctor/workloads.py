'''
Created on Mar 29, 2024

@author: ritesh.agarwal
'''
from TestInput import TestInputSingleton

_input = TestInputSingleton.input

default = {
    "valType": _input.param("val_type", "SimpleValue"),
    "scopes": 1,
    "collections": _input.param("collections", 2),
    "num_items": _input.param("num_items", 50000000),
    "start": 0,
    "end": _input.param("num_items", 50000000),
    "ops": _input.param("ops_rate", 50000),
    "doc_size": _input.param("doc_size", 1024),
    "pattern": [0, 50, 50, 0, 0], # CRUDE
    "load_type": ["read", "update"],
    "2iQPS": 1,
    "ftsQPS": 10,
    "cbasQPS": 10,
    "collections_defn": [
        {
            "valType": _input.param("val_type", "SimpleValue"),
            "2i": [_input.param("gsi_indexes", 2),
                   _input.param("gsi_queries", 2)],
            "FTS": [_input.param("fts_indexes", 2),
                    _input.param("fts_queries", 2)],
            "cbas": [_input.param("cbas_indexes", 2),
                     _input.param("cbas_datasets", 2),
                     _input.param("cbas_queries", 2)]
            }
        ]
    }
        
nimbus = {
    "valType": "Hotel",
    "scopes": 1,
    "collections": 2,
    "num_items": _input.param("num_items", 1500000000),
    "start": 0,
    "end": _input.param("num_items", 1500000000),
    "ops": _input.param("ops_rate", 100000),
    "doc_size": 1024,
    "pattern": [0, 50, 50, 0, 0],  # CRUDE
    "load_type": ["read", "update"],
    "2iQPS": 300,
    "ftsQPS": 100,
    "cbasQPS": 100,
    "collections_defn": [
        {
            "valType": "NimbusP",
            "2i": [2, 3],
            "FTS": [0, 0],
            "cbas": [0, 0, 0],
            "indexes": [
                'CREATE INDEX {}{} ON {}(`uid`, `lastMessageDate` DESC,`unreadCount`, `lastReadDate`, `conversationId`) PARTITION BY hash(`uid`) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`conversationId`, `uid`) PARTITION BY hash(`conversationId`) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};'
                ],
            "queries": [
                'SELECT meta().id, conversationId, lastMessageDate, lastReadDate, unreadCount FROM {} WHERE uid = $uid ORDER BY lastMessageDate DESC LIMIT $N;',
                'SELECT uid, conversationId FROM {} WHERE conversationId IN [$conversationId1, $conversationId2]',
                'SELECT COUNT(*) AS nb FROM {}  WHERE uid=$uid AND unreadCount>0'
                ]
            },
        {
            "valType": "NimbusM",
            "2i": [1, 2],
            "FTS": [0, 0],
            "cbas": [0, 0, 0],
            "indexes": [
                'CREATE INDEX {}{} ON {}(`conversationId`, `uid`) PARTITION BY hash(`conversationId`) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`conversationId`, (distinct (array `u` for `u` in `showTo` end)), `timestamp` DESC) PARTITION BY hash(`conversationId`) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};'
                ],
            "queries": [
                'SELECT meta().id AS _id, uid, type, content, url, timestamp, width, height, clickable, roomId, roomTitle, roomStreamers, actions, pixel FROM {} WHERE conversationId = $conversationId ORDER BY timestamp DESC LIMIT $N',
                'SELECT COUNT(*) AS nb FROM {} WHERE conversationId = $conversationId'
                ]
            }
        ]
    }

siftBigANN = {
    "valType": "siftBigANN",
    "baseFilePath": _input.param("baseFilePath", "/root/bigann"),
    "scopes": 1,
    "collections": _input.param("collections", 2),
    "ops": _input.param("ops_rate", 50000),
    "doc_size": _input.param("doc_size", 1024),
    "pattern": [0, 0, 100, 0, 0], # CRUDE
    "load_type": ["update"],
    "2iQPS": 50,
    "ftsQPS": 10,
    "cbasQPS": 10,
    "collections_defn": [
        {
            "collection_id": "10M",
            "num_items": 10000000,
            "valType": "siftBigANN",
            "dim": _input.param("dim", 128),
            "vector": [
                {
                    "similarity": _input.param("similarity", "L2_SQUARED"),
                    "quantization": _input.param("quantization", "SQ8"),
                    "nProbe": _input.param("nProbe", 20)
                    }
                ],
            "indexes": [
                'CREATE INDEX {index_name} ON {collection}(`brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`brand`, `color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries": [
                'SELECT id from {collection} where brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {collection}(`brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries_base64": [
                'SELECT id from {collection} where brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "mix_indexes": [
                'CREATE INDEX {index_name} ON {collection}(`brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(decode_vector(`embedding`, false) VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "mix_queries": [
                'SELECT id from {collection} where brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10'
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/10"),
                ("idx_2M.ivecs", "2/10"),
                ("idx_5M.ivecs", "5/10"),
                ],
            "2i": [_input.param("gsi_indexes", 3),
                   _input.param("gsi_queries", 3)],
            "es": [{"brand.keyword": "Nike", "color.keyword": "Green", "size":5}, {"brand.keyword": "Nike", "color.keyword": "Green"}, {"brand.keyword": "Nike"}]
        },
        {
            "collection_id": "20M",
            "num_items": 20000000,
            "valType": "siftBigANN",
            "dim": _input.param("dim", 128),
            "vector": [
                {
                    "similarity": _input.param("similarity", "L2_SQUARED"),
                    "quantization": _input.param("quantization", "SQ8"),
                    "nProbe": _input.param("nProbe", 20)
                    }
                ],
            "indexes": [
                'CREATE INDEX {index_name} ON {collection}(`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`country`, `brand`, `color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`country`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (country, brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (country, brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "mix_indexes": [
                'CREATE INDEX {index_name} ON {collection}(`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT id from {collection} where country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10'
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {collection}(`country`, `brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`country`, `brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`country`, `brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`country`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries_base64": [
                'SELECT id from {collection} where country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10'
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/20"),
                ("idx_2M.ivecs", "2/20"),
                ("idx_5M.ivecs", "5/20"),
                ("idx_10M.ivecs", "10/20"),
                ],
            "2i": [_input.param("gsi_indexes", 4),
                   _input.param("gsi_queries", 4)],
        },
        {
            "collection_id": "50M",
            "num_items": 50000000,
            "valType": "siftBigANN",
            "dim": _input.param("dim", 128),
            "vector": [
                {
                    "similarity": _input.param("similarity", "L2_SQUARED"),
                    "quantization": _input.param("quantization", "SQ8"),
                    "nProbe": _input.param("nProbe", 20)
                    }
                ],
            "indexes": [
                'CREATE INDEX {index_name} ON {collection}(`category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`,`country`, `brand`, `color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`,`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`,`country`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (category, country, brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (category, country, brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (category, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "mix_indexes": [
                'CREATE INDEX {index_name} ON {collection}(`category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`,`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT id from {collection} where category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {collection}(`category`,`country`, `brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`,`country`, `brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`,`country`, `brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`,`country`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`,decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries_base64": [
                'SELECT id from {collection} where category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/50"),
                ("idx_2M.ivecs", "2/50"),
                ("idx_5M.ivecs", "5/50"),
                ("idx_10M.ivecs", "10/50"),
                ("idx_20M.ivecs", "20/50"),
                ],
            "2i": [_input.param("gsi_indexes", 5),
                   _input.param("gsi_queries", 5)],
        },
        {
            "collection_id": "100M",
            "num_items": 100000000,
            "valType": "siftBigANN",
            "dim": _input.param("dim", 128),
            "vector": [
                {
                    "similarity": _input.param("similarity", "L2_SQUARED"),
                    "quantization": _input.param("quantization", "SQ8"),
                    "nProbe": _input.param("nProbe", 20)
                    }
                ],
            "indexes": [
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`, `country`, `brand`, `color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`,`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`,`country`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, category, country, brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, category, country, brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, category, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "mix_indexes": [
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`,`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT id from {collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where `type`="Casual" AND category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where `type`="Casual" AND category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`,`country`, `brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`, `country`, `brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`, `country`, `brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`,`country`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, `category`,decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`type`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries_base64": [
                'SELECT id from {collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where `type`="Casual" AND category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where `type`="Casual" AND category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/100"),
                ("idx_2M.ivecs", "2/100"),
                ("idx_5M.ivecs", "5/100"),
                ("idx_10M.ivecs", "10/100"),
                ("idx_20M.ivecs", "20/100"),
                ("idx_50M.ivecs", "50/100"),
                ],
            "2i": [_input.param("gsi_indexes", 6),
                   _input.param("gsi_queries", 6)],
            "es": [{"type.keyword": "Casual","category.keyword": "Shoes", "country.keyword": "USA", "brand.keyword": "Nike", "color.keyword": "Green", "size":5},
                   {"type.keyword": "Casual","category.keyword": "Shoes", "country.keyword": "USA", "brand.keyword": "Nike", "color.keyword": "Green"},
                   {"type.keyword": "Casual","category.keyword": "Shoes", "country.keyword": "USA", "brand.keyword": "Nike"},
                   {"type.keyword": "Casual","category.keyword": "Shoes", "country.keyword": "USA"},
                   {"type.keyword": "Casual","category.keyword": "Shoes"},
                   {"type.keyword": "Casual"}
                   ]
        },
        {
            "collection_id": "200M",
            "num_items": 200000000,
            "valType": "siftBigANN",
            "dim": _input.param("dim", 128),
            "vector": [
                {
                    "similarity": _input.param("similarity", "L2_SQUARED"),
                    "quantization": _input.param("quantization", "SQ8"),
                    "nProbe": _input.param("nProbe", 20)
                    }
                ],
            "indexes": [
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `brand`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, type, category, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
                "mix_indexes": [
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `brand`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `brand`, decode_vector(`embedding`, false) VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`review`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries_base64": [
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/200"),
                ("idx_2M.ivecs", "2/200"),
                ("idx_5M.ivecs", "5/200"),
                ("idx_10M.ivecs", "10/200"),
                ("idx_20M.ivecs", "20/200"),
                ("idx_50M.ivecs", "50/200"),
                ("idx_100M.ivecs", "100/200"),
                ],
            "2i": [_input.param("gsi_indexes", 6),
                   _input.param("gsi_queries", 6)],
            "es": [{"review": 1.0, "type.keyword": "Casual","category.keyword": "Shoes", "country.keyword": "USA", "brand.keyword": "Nike", "color.keyword": "Green", "size":5},
                   {"review": 1.0, "type.keyword": "Casual","category.keyword": "Shoes", "country.keyword": "USA", "brand.keyword": "Nike", "color.keyword": "Green"},
                   {"review": 1.0, "type.keyword": "Casual","category.keyword": "Shoes", "country.keyword": "USA", "brand.keyword": "Nike"},
                   {"review": 1.0, "type.keyword": "Casual","category.keyword": "Shoes", "country.keyword": "USA"},
                   {"review": 1.0, "type.keyword": "Casual","category.keyword": "Shoes"},
                   {"review": 1.0, "type.keyword": "Casual"},
                   {"review": 1.0}
                   ]
        },
        {
            "collection_id": "500M",
            "num_items": 500000000,
            "valType": "siftBigANN",
            "dim": _input.param("dim", 128),
            "vector": [
                {
                    "similarity": _input.param("similarity", "L2_SQUARED"),
                    "quantization": _input.param("quantization", "SQ8"),
                    "nProbe": _input.param("nProbe", 20)
                    }
                ],
            "indexes": [
                'CREATE INDEX {index_name} ON {collection}(`size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`country`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE INDEX {index_name} ON {collection}(`type`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE INDEX {index_name} ON {collection}(`review`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (category, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (review, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "mix_indexes": [
                'CREATE INDEX {index_name} ON {collection}(`size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE INDEX {index_name} ON {collection}(`review`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries": [
                'SELECT id from {collection} where size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review=1 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review in [1.0, 2.0] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {collection}(`size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`country`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`category`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE INDEX {index_name} ON {collection}(`type`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE INDEX {index_name} ON {collection}(`review`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries_base64": [
                'SELECT id from {collection} where size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review=1 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review in [1.0, 2.0] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/500"),
                ("idx_2M.ivecs", "2/500"),
                ("idx_5M.ivecs", "5/500"),
                ("idx_10M.ivecs", "10/500"),
                ("idx_20M.ivecs", "20/500"),
                # ("idx_50M.ivecs", "50/500"),
                # ("idx_100M.ivecs", "100/500"),
                # ("idx_200M.ivecs", "200/500"),
                ],
            "2i": [_input.param("gsi_indexes", 5),
                   _input.param("gsi_queries", 5)],
        },
        {
            "collection_id": "1B",
            "num_items": 1000000000,
            "valType": "siftBigANN",
            "dim": _input.param("dim", 128),
            "vector": [
                {
                    "similarity": _input.param("similarity", "L2_SQUARED"),
                    "quantization": _input.param("quantization", "SQ8"),
                    "description": "IVF100000,SQ8",
                    "nProbe": _input.param("nProbe", 20)
                    }
                ],
            # "indexes": [
            #     'CREATE INDEX {index_name} ON {collection}(`embedding` VECTOR, `review`, `type`, `category`,`country`, `brand`, `color`, `size`, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
            #     ],
            # "queries": [
            #     'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {collection} where color="Green" AND size in [5,6] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {collection} where brand="Nike" AND color in ["Green","Red"] AND size in [5,6,7] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {collection} where `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {collection} where review=1.0 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {collection} where review in [1.0, 2.0]ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {collection} where review < 6  ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {collection} where review > 0 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     ],
            "indexes": [
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {collection}(`embedding` VECTOR) INCLUDE (`review`, `type`, `category`,`country`, `brand`, `color`, `size`, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries": [
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size in [5,6] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color in ["Green","Red"] AND size in [5,6,7] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand in ["Nike","Adidas"] AND color in ["Green","Red", "Blue"] AND size in [5,6,7,8] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country in ["USA","Canada"] AND brand in ["Nike","Adidas", "Puma"] AND color in ["Green","Red", "Blue","Purple"] AND size in [5,6,7,8,9] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category in ["Shoes","Jeans"] AND country in ["USA","Canada","Australia"] AND brand in ["Nike","Adidas","Puma","Asic"] AND color in ["Green","Red", "Blue","Purple","Pink"] AND size in [5,6,7,8,9,10] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review=1.0 AND `type` in ["Casual","Formal"] AND category in ["Shoes","Jeans","Shirt"] AND country in ["USA","Canada","Australia","England"] AND brand in ["Nike","Adidas","Puma","Asic","Brook"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow"] AND size in [5,6,7,8,9,10,11] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review in [1.0, 2.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review in [1.0, 2.0, 5.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review in [1.0, 2.0, 5.0, 10.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "queries_base64": [
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size in [5,6] ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color in ["Green","Red"] AND size in [5,6,7] ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand in ["Nike","Adidas"] AND color in ["Green","Red", "Blue"] AND size in [5,6,7,8] ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country in ["USA","Canada"] AND brand in ["Nike","Adidas", "Puma"] AND color in ["Green","Red", "Blue","Purple"] AND size in [5,6,7,8,9] ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {collection} where review=1.0 AND `type`="Casual" AND category in ["Shoes","Jeans"] AND country in ["USA","Canada","Australia"] AND brand in ["Nike","Adidas","Puma","Asic"] AND color in ["Green","Red", "Blue","Purple","Pink"] AND size in [5,6,7,8,9,10] ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review=1.0 AND `type` in ["Casual","Formal"] AND category in ["Shoes","Jeans","Shirt"] AND country in ["USA","Canada","Australia","England"] AND brand in ["Nike","Adidas","Puma","Asic","Brook"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow"] AND size in [5,6,7,8,9,10,11] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review in [1.0, 2.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review in [1.0, 2.0, 5.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {collection} where review in [1.0, 2.0, 5.0, 10.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/1B"),
                ("idx_2M.ivecs", "2/1B"),
                ("idx_5M.ivecs", "5/1B"),
                ("idx_10M.ivecs", "10/1B"),
                ("idx_20M.ivecs", "20/1B"),
                ("idx_50M.ivecs", "50/1B"),
                # ("idx_100M.ivecs", "100/1B"),
                # ("idx_200M.ivecs", "200/1B"),
                # ("idx_500M.ivecs", "500/1B"),
                # ("idx_1000M.ivecs", "1B/1B")
                ],
            "2i": [_input.param("gsi_indexes", 1),
                   _input.param("gsi_queries", 6)],
        },
    ]
}

hotel_vector = {
    "valType": _input.param("val_type", "Hotel"),
    "scopes": 1,
    "collections": _input.param("collections", 2),
    "num_items": _input.param("num_items", 50000000),
    "start": 0,
    "end": _input.param("num_items", 50000000),
    "ops": _input.param("ops_rate", 50000),
    "doc_size": _input.param("doc_size", 1024),
    "pattern": [0, 0, 100, 0, 0], # CRUDE
    "load_type": _input.param("doc_ops", ["update"]),
    "2iQPS": 10,
    "ftsQPS": 10,
    "cbasQPS": 10,
    "collections_defn": [
        {
            "valType": _input.param("val_type", "Hotel"),
            "dim": _input.param("dim", 128),
            "vector": [
                {
                    "similarity": _input.param("similarity", "L2_SQUARED"),
                    "quantization": _input.param("quantization", "PQ32x8"),
                    "nProbe": _input.param("nProbe", 3)
                    },
                {
                    "similarity": _input.param("similarity", "L2_SQUARED"),
                    "quantization": _input.param("quantization", "SQ8"),
                    "nProbe": _input.param("nProbe", 3)
                    },
                {
                    "similarity": _input.param("similarity", "L2"),
                    "quantization": _input.param("quantization", "PQ32x8"),
                    "nProbe": _input.param("nProbe", 5)
                    },
                {
                    "similarity": _input.param("similarity", "L2"),
                    "quantization": _input.param("quantization", "SQ8"),
                    "nProbe": _input.param("nProbe", 5)
                    },
                {
                    "similarity": _input.param("similarity", "EUCLIDEAN"),
                    "quantization": _input.param("quantization", "PQ32x8"),
                    "nProbe": _input.param("nProbe", 10)
                    },
                {
                    "similarity": _input.param("similarity", "EUCLIDEAN_SQUARED"),
                    "quantization": _input.param("quantization", "PQ32x8"),
                    "nProbe": _input.param("nProbe", 20)
                    }
                ],
            "indexes": [
                'CREATE INDEX {index_name} ON {collection}(`country`,`vector` VECTOR) PARTITION BY hash((meta().`id`)) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`free_breakfast`,`type`,`free_parking`,array_count((`public_likes`)),`price`,`country`, vector VECTOR) PARTITION BY HASH (type) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`free_breakfast`,`free_parking`,`country`,`city`, vector VECTOR)  PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`country`, `city`,`price`,`name`, vector VECTOR)  PARTITION BY HASH (country, city) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(vector VECTOR, `price`,`name`,`city`,`country`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`name` INCLUDE MISSING DESC,`phone`,`type`, vector VECTOR) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`city` INCLUDE MISSING ASC, `phone`, vector VECTOR) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {collection}(`avg_rating`, `price`, `country`, `city`, vector VECTOR) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT meta().id from {collection} WHERE country=$country ORDER BY APPROX_VECTOR_DISTANCE(vector, $vector, "{similarity}", {nProbe}) limit 100',
                'SELECT price, country from {collection} where free_breakfast=True and `type`= $type AND free_parking=True and price is not null and array_count(public_likes)>=0 ORDER BY APPROX_VECTOR_DISTANCE(vector, $vector, "{similarity}", {nProbe}) limit 100',
                'SELECT city,country from {collection} where free_breakfast=True and free_parking=True order by APPROX_VECTOR_DISTANCE(vector, $vector, "{similarity}", {nProbe}) limit 100',
                'SELECT COUNT(1) AS cnt FROM {collection} WHERE city LIKE "North%" ORDER BY APPROX_VECTOR_DISTANCE(vector, $vector, "{similarity}", {nProbe}) limit 100',
                'SELECT h.name,h.country,h.city,h.price FROM {collection} AS h WHERE h.price IS NOT NULL ORDER BY APPROX_VECTOR_DISTANCE(vector, $vector, "{similarity}", {nProbe}) limit 100',
                'SELECT * from {collection} where `name` is not null ORDER BY APPROX_VECTOR_DISTANCE(vector, $vector, "{similarity}", {nProbe}) limit 100',
                'SELECT * from {collection} where city like \"San%\" ORDER BY APPROX_VECTOR_DISTANCE(vector, $vector, "{similarity}", {nProbe}) limit 100',
                'SELECT avg_rating, price, country, city from {collection} where `avg_rating`>4 and price<1000 and country=$country and city=$city ORDER BY APPROX_VECTOR_DISTANCE(vector, $vector, "{similarity}", {nProbe}) LIMIT 100'
                ],
            "2i": [_input.param("gsi_indexes", 2),
                   _input.param("gsi_queries", 2)]
            },
        ],
    }

vector_load = {
    "valType": "Vector",
    "scopes": 1,
    "collections": _input.param("collections", 2),
    "num_items": _input.param("num_items", 5000000),
    "start": 0,
    "end": _input.param("num_items", 5000000),
    "ops": _input.param("ops_rate", 5000),
    "doc_size": 1024,
    "pattern": [0, 0, 100, 0, 0], # CRUDE
    "load_type": ["update"],
    "2iQPS": 10,
    "ftsQPS": _input.param("ftsQPS", 10),
    "cbasQPS": 0,
    "collections_defn": [
        {
            "valType": "Vector",
            "2i": [2, 2],
            "FTS": [2, 2],
            "cbas": [2, 2, 2]
            }
        ]
    }

quartz1 = {
    "name": "archive",
    "ramQuotaMB": 1024,
    "valType": "Hotel",
    "scopes": 1,
    "collections": 1,
    "num_items": 5000000,
    "start": 0,
    "end": 5000000,
    "ops": _input.param("ops_rate", 10000),
    "doc_size": 1024,
    "pattern": [10, 0, 80, 10, 0],  # CRUDE
    "load_type": ["create", "update", "delete"],
    "2iQPS": 5,
    "ftsQPS": 0,
    "cbasQPS": 0,
    "collections_defn": [
        {
            "valType": "Hotel",
            "2i": [5, 5],
            "FTS": [0, 0],
            "cbas": [0, 0, 0]
            }
        ]
    }

quartz2 = {
    "name": "backend",
    "ramQuotaMB": 40960,
    "valType": "Hotel",
    "scopes": 1,
    "collections": 1,
    "num_items": 14000000,
    "start": 0,
    "end": 14000000,
    "ops": _input.param("ops_rate", 10000),
    "doc_size": 1024,
    "pattern": [10, 0, 80, 10, 0],  # CRUDE
    "load_type": ["create", "update", "delete"],
    "2iQPS": 5,
    "ftsQPS": 0,
    "cbasQPS": 0,
    "collections_defn": [
        {
            "valType": "Hotel",
            "2i": [15, 10],
            "FTS": [0, 0],
            "cbas": [0, 0, 0]
            }
        ]
    }

quartz3 = {
    "name": "export",
    "ramQuotaMB": 3072,
    "valType": "SimpleValue",
    "scopes": 1,
    "collections": 1,
    "num_items": 2000000,
    "start": 0,
    "end": 2000000,
    "ops": _input.param("ops_rate", 10000),
    "doc_size": 1024,
    "pattern": [10, 0, 80, 10, 0],  # CRUDE
    "load_type": ["create", "update", "delete"],
    "2iQPS": 5,
    "ftsQPS": 0,
    "cbasQPS": 0,
    "collections_defn": [
        {
            "valType": "SimpleValue",
            "2i": [9, 9],
            "FTS": [0, 0],
            "cbas": [0, 0, 0]
            }
        ]
    }

quartz4 = {
    "name": "import",
    "ramQuotaMB": 10240,
    "valType": "Hotel",
    "scopes": 1,
    "collections": 1,
    "num_items": 1200000,
    "start": 0,
    "end": 1200000,
    "ops": _input.param("ops_rate", 10000),
    "doc_size": 1024,
    "pattern": [10, 0, 80, 10, 0],  # CRUDE
    "load_type": ["create", "update", "delete"],
    "2iQPS": 5,
    "ftsQPS": 0,
    "cbasQPS": 0,
    "collections_defn": [
        {
            "valType": "Hotel",
            "2i": [5, 5],
            "FTS": [0, 0],
            "cbas": [0, 0, 0]
            }
        ]
    }

quartz5 = {
    "name": "sync",
    "ramQuotaMB": 40960,
    "valType": "Hotel",
    "scopes": 1,
    "collections": 1,
    "num_items": 20000000,
    "start": 0,
    "end": 20000000,
    "ops": _input.param("ops_rate", 10000),
    "doc_size": 1024,
    "pattern": [10, 0, 80, 10, 0],  # CRUDE
    "load_type": ["create", "update", "delete"],
    "2iQPS": 5,
    "ftsQPS": 0,
    "cbasQPS": 0,
    "collections_defn": [
        {
            "valType": "Hotel",
            "2i": [100, 10],
            "FTS": [0, 0],
            "cbas": [0, 0, 0]
            }
        ]
    }

quartz6 = {
    "name": "timelines",
    "ramQuotaMB": 81920,
    "valType": "SimpleValue",
    "scopes": 1,
    "collections": 1,
    "num_items": 8000000,
    "start": 0,
    "end": 8000000,
    "ops": _input.param("ops_rate", 10000),
    "doc_size": 1024,
    "pattern": [10, 0, 80, 10, 0],  # CRUDE
    "load_type": ["create", "update", "delete"],
    "2iQPS": 0,
    "ftsQPS": 0,
    "cbasQPS": 0,
    "collections_defn": [
        {
            "valType": "SimpleValue",
            "2i": [0, 0],
            "FTS": [0, 0],
            "cbas": [0, 0, 0]
            }
        ]
    }
