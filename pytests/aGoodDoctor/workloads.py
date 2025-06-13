'''
Created on Mar 29, 2024

@author: ritesh.agarwal
'''
from TestInput import TestInputSingleton

_input = TestInputSingleton.input
        
default = {
    "key_prefix": "test_docs-",
    "key_size": 32,
    "key_type": _input.param("key_type", "SimpleKey"),
    "valType": _input.param("valType", "Hotel"),
    "collections": _input.param("collections", 2),
    "scopes": 1,
    "num_items": _input.param("num_items", 50000000),
    "start": 0,
    "end": _input.param("num_items", 50000000),
    "ops": _input.param("ops_rate", 50000),
    "doc_size": _input.param("doc_size", 1024),
    "doc_op_percentages": {"create": 0, "update": 50, "delete": 0, "read": 50, "expiry": 0},
    "2iQPS": _input.param("2iQPS", 10),
    "ftsQPS": _input.param("ftsQPS", 10),
    "cbasQPS": _input.param("cbasQPS", 10),
    "collections_defn": [
        {
            "valType": _input.param("val_type", "SimpleValue"),
            "2i": {"num_queries": _input.param("2i_queries", 2),
                   "num_indexes": _input.param("2i_indexes", 2)},
            "FTS": {"num_queries": _input.param("fts_queries", 2),
                    "num_indexes": _input.param("fts_indexes", 2)},
            "cbas": {"num_collections": _input.param("cbas_collections", 2),
                     "num_queries": _input.param("cbas_queries", 5),
                     "num_indexes": _input.param("cbas_indexes", 0)},
            }
        ]
    }

Hotel = {
    "key_prefix": "test_docs-",
    "key_size": 20,
    "key_type": _input.param("key_type", "SimpleKey"),
    "valType": "Hotel",
    "scopes": 1,
    "collections": 2,
    "num_items": _input.param("num_items", 1000000000),
    "start": 0,
    "end": _input.param("num_items", 1000000000),
    "ops": _input.param("ops_rate", 100000),
    "doc_size": 1024,
    "doc_op_percentages": {"create": 0, "update": 50, "delete": 0, "read": 50, "expiry": 0},
    "2iQPS": _input.param("2iQPS", 300),
    "ftsQPS": _input.param("ftsQPS", 100),
    "cbasQPS": _input.param("cbasQPS", 100),
    "collections_defn": [
        {
            "valType": "Hotel",
            "2i": {"num_queries": _input.param("2i_queries", 2),
                   "num_indexes": _input.param("2i_indexes", 3)},
            "FTS": {"num_queries": _input.param("fts_queries", 0),
                    "num_indexes": _input.param("fts_indexes", 0)},
            "cbas": {"num_collections": _input.param("cbas_collections", 0),
                     "num_queries": _input.param("cbas_queries", 0),
                     "num_indexes": _input.param("cbas_indexes", 0)},
            "indexes": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(country, DISTINCT ARRAY `r`.`ratings`.`Check in / front desk` FOR r in `reviews` END,array_count((`public_likes`)),array_count((`reviews`)) DESC,`type`,`phone`,`price`,`email`,`address`,`name`,`url`) PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`free_breakfast`,`type`,`free_parking`,array_count((`public_likes`)),`price`,`country`) PARTITION BY HASH (type) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`free_breakfast`,`free_parking`,`country`,`city`)  PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `city`,`price`,`name`)  PARTITION BY HASH (country, city) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(ALL ARRAY `r`.`ratings`.`Rooms` FOR r IN `reviews` END,`avg_rating`)  PARTITION BY HASH (avg_rating) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`city`) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`price`,`name`,`city`,`country`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`name` INCLUDE MISSING DESC,`phone`,`type`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `free_parking`, DISTINCT ARRAY FLATTEN_KEYS(`r`.`ratings`.`Cleanliness`,`r`.`author`) FOR r IN `reviews` when `r`.`ratings`.`Cleanliness` < 4 END, `email`) PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Inn USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Hostel USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Place USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Center USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Hotel USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Motel USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Suites USING GSI WITH {{ "defer_build": true}};'
                ],
            "queries": [
                "select meta().id from {bucket}.{scope}.{collection} where country is not null and `type` is not null and (any r in reviews satisfies r.ratings.`Check in / front desk` is not null end) limit 100",
                "select price, country from {bucket}.{scope}.{collection} where free_breakfast=True AND free_parking=True and price is not null and array_count(public_likes)>=0 and `type`= $type limit 100",
                "select city,country from {bucket}.{scope}.{collection} where free_breakfast=True and free_parking=True order by country,city limit 100",
                "WITH city_avg AS (SELECT city, AVG(price) AS avgprice FROM {bucket}.{scope}.{collection} WHERE country = $country GROUP BY city limit 10) SELECT h.name, h.price FROM city_avg JOIN {bucket}.{scope}.{collection} h ON h.city = city_avg.city WHERE h.price < city_avg.avgprice AND h.country=$country limit 100",
                "SELECT h.name, h.city, r.author FROM {bucket}.{scope}.{collection} h UNNEST reviews AS r WHERE r.ratings.Rooms = 2 AND h.avg_rating >= 3 limit 100",
                "SELECT COUNT(1) AS cnt FROM {bucket}.{scope}.{collection} WHERE city LIKE 'North%'",
                "SELECT h.name,h.country,h.city,h.price FROM {bucket}.{scope}.{collection} AS h WHERE h.price IS NOT NULL limit 100",
                "SELECT * from {bucket}.{scope}.{collection} where `name` is not null limit 100",
                "SELECT * from {bucket}.{scope}.{collection} where city like \"San%\" limit 100",
                "SELECT * FROM {bucket}.{scope}.{collection} AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND free_parking = TRUE AND country = $country limit 100",
                "SELECT ARRAY_AGG({{h.name, r.ratings.Overall}}) AS reviews FROM {bucket}.{scope}.{collection} h UNNEST reviews AS r limit 100"
                ],
            "queryParams": [
                {"country": "faker.address().country()"},
                {"type": "random.choice(['Inn', 'Hostel', 'Place', 'Center', 'Hotel', 'Motel', 'Suites'])"},
                {},
                {},
                {},
                {},
                {},
                {"country": "faker.address().country()", "city": "faker.address().city()"}
                ]
            }
        ]
}

nimbus = {
    "key_prefix": "test_docs-",
    "key_size": 20,
    "key_type": _input.param("key_type", "SimpleKey"),
    "valType": _input.param("valType", "Hotel"),
    "scopes": 1,
    "collections": 2,
    "num_items": _input.param("num_items", 1500000000),
    "start": 0,
    "end": _input.param("num_items", 1500000000),
    "ops": _input.param("ops_rate", 100000),
    "doc_size": 1024,
    "doc_op_percentages": {"create": 0, "update": 50, "delete": 0, "read": 50, "expiry": 0},
    "2iQPS": _input.param("2iQPS", 300),
    "ftsQPS": _input.param("ftsQPS", 100),
    "cbasQPS": _input.param("cbasQPS", 100),
    "collections_defn": [
        {
            "valType": "NimbusP",
            "2i": {"num_queries": _input.param("2i_queries", 2),
                   "num_indexes": _input.param("2i_indexes", 3)},
            "FTS": {"num_queries": _input.param("fts_queries", 0),
                    "num_indexes": _input.param("fts_indexes", 0)},
            "cbas": {"num_collections": _input.param("cbas_collections", 0),
                     "num_queries": _input.param("cbas_queries", 0),
                     "num_indexes": _input.param("cbas_indexes", 0)},
            "indexes": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`uid`, `lastMessageDate` DESC,`unreadCount`, `lastReadDate`, `conversationId`) PARTITION BY hash(`uid`) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`conversationId`, `uid`) PARTITION BY hash(`conversationId`) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};'
                ],
            "queries": [
                'SELECT meta().id, conversationId, lastMessageDate, lastReadDate, unreadCount FROM {bucket}.{scope}.{collection} WHERE uid = $uid ORDER BY lastMessageDate DESC LIMIT $N;',
                'SELECT uid, conversationId FROM {bucket}.{scope}.{collection} WHERE conversationId IN [$conversationId1, $conversationId2]',
                'SELECT COUNT(*) AS nb FROM {bucket}.{scope}.{collection}  WHERE uid=$uid AND unreadCount>0'
                ],
            "queryParams": [
                {"uid": "faker.random_int(min=1, max=1000000000)"},
                {"conversationId1": "faker.random_int(min=1, max=1000000000)", "conversationId2": "faker.random_int(min=1, max=1000000000)"},
                {"uid": "faker.random_int(min=1, max=1000000000)"}
                ]
            },
        {
            "valType": "NimbusM",
            "2i": {"num_queries": _input.param("2i_queries", 1),
                   "num_indexes": _input.param("2i_indexes", 2)},
            "FTS": {"num_queries": _input.param("fts_queries", 0),
                    "num_indexes": _input.param("fts_indexes", 0)},
            "cbas": {"num_collections": _input.param("cbas_collections", 0),
                     "num_queries": _input.param("cbas_queries", 0),
                     "num_indexes": _input.param("cbas_indexes", 0)},
            "indexes": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`conversationId`, `uid`) PARTITION BY hash(`conversationId`) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`conversationId`, (distinct (array `u` for `u` in `showTo` end)), `timestamp` DESC) PARTITION BY hash(`conversationId`) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};'
                ],
            "queries": [
                'SELECT meta().id AS _id, uid, type, content, url, timestamp, width, height, clickable, roomId, roomTitle, roomStreamers, actions, pixel FROM {bucket}.{scope}.{collection} WHERE conversationId = $conversationId ORDER BY timestamp DESC LIMIT $N',
                'SELECT COUNT(*) AS nb FROM {bucket}.{scope}.{collection} WHERE conversationId = $conversationId'
                ],
            "queryParams": [
                {"conversationId": "faker.random_int(min=1, max=1000000000)"},
                {"conversationId": "faker.random_int(min=1, max=1000000000)"}
                ]
            }
        ]
    }

siftBigANN = {
    "key_prefix": "test_docs-",
    "key_size": 32,
    "key_type": _input.param("key_type", "SimpleKey"),
    "valType": "siftBigANN",
    "baseFilePath": _input.param("baseFilePath", "/root/bigann"),
    "scopes": 1,
    "collections": _input.param("collections", 2),
    "ops": _input.param("ops_rate", 50000),
    "doc_size": _input.param("doc_size", 1024),
    "doc_op_percentages": {"create": 0, "update": 100, "delete": 0, "read": 0, "expiry": 0},
    "2iQPS": _input.param("2iQPS", 50),
    "ftsQPS": _input.param("ftsQPS", 10),
    "cbasQPS": _input.param("cbasQPS", 10),
    "collections_defn": [
        {
            "collection_id": "1M",
            "num_items": 1000000,
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
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries_base64": [
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10'
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/1"),
                ("idx_1M.ivecs", "1/1"),
                ("idx_1M.ivecs", "1/1"),
                ("idx_1M.ivecs", "1/1")
                ],
            "2i": {"num_queries": _input.param("2i_queries", 4),
                   "num_indexes": _input.param("2i_indexes", 4)},
            "es": [{"brand.keyword": "Nike", "color.keyword": "Green", "size":5}, {"brand.keyword": "Nike", "color.keyword": "Green"}, {"brand.keyword": "Nike"}]
        },
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
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries_base64": [
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10'
                ],
            "mix_indexes": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(decode_vector(`embedding`, false) VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "mix_queries": [
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10'
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/10"),
                ("idx_2M.ivecs", "2/10"),
                ("idx_5M.ivecs", "5/10"),
                ("idx_10M.ivecs", "10/10")
                ],
            "2i": {"num_queries": _input.param("2i_queries", 4),
                   "num_indexes": _input.param("2i_indexes", 4)},
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
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `brand`, `color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (country, brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (country, brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "mix_indexes": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT id from {bucket}.{scope}.{collection} where country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries_base64": [
                'SELECT id from {bucket}.{scope}.{collection} where country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/20"),
                ("idx_2M.ivecs", "2/20"),
                ("idx_5M.ivecs", "5/20"),
                ("idx_10M.ivecs", "10/20"),
                ("idx_20M.ivecs", "20/20"),
                ],
            "2i": {"num_queries": _input.param("2i_queries", 5),
                   "num_indexes": _input.param("2i_indexes", 5)},
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
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`country`, `brand`, `color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`country`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (category, country, brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (category, country, brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (category, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "mix_indexes": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`country`, `brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`country`, `brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`country`, `brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,`country`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`,decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries_base64": [
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10'
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/50"),
                ("idx_2M.ivecs", "2/50"),
                ("idx_5M.ivecs", "5/50"),
                ("idx_10M.ivecs", "10/50"),
                ("idx_20M.ivecs", "20/50"),
                ("idx_50M.ivecs", "50/50"),
                ],
            "2i": {"num_queries": _input.param("2i_queries", 6),
                   "num_indexes": _input.param("2i_indexes", 6)},
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
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`, `country`, `brand`, `color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`,`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`,`country`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, category, country, brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, category, country, brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, category, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "mix_indexes": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`,`country`, `brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" AND category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" AND category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`,`country`, `brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`, `country`, `brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`, `country`, `brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`,`country`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `category`,decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries_base64": [
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" AND category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" AND category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10'
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/100"),
                ("idx_2M.ivecs", "2/100"),
                ("idx_5M.ivecs", "5/100"),
                ("idx_10M.ivecs", "10/100"),
                ("idx_20M.ivecs", "20/100"),
                ("idx_50M.ivecs", "50/100"),
                ("idx_100M.ivecs", "100/100"),
                ],
            "2i": {"num_queries": _input.param("2i_queries", 7),
                   "num_indexes": _input.param("2i_indexes", 7)},
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
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `brand`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, brand, color, size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, type, category, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
                "mix_indexes": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, brand, color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `brand`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, type, category, country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries": [
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `brand`, `color`, decode_vector(`embedding`, false) VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `brand`, decode_vector(`embedding`, false) VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "queries_base64": [
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "groundTruths":[
                ("idx_1M.ivecs", "1/200"),
                ("idx_2M.ivecs", "2/200"),
                ("idx_5M.ivecs", "5/200"),
                ("idx_10M.ivecs", "10/200"),
                ("idx_20M.ivecs", "20/200"),
                ("idx_50M.ivecs", "50/200"),
                ("idx_100M.ivecs", "100/200"),
                ("idx_200M.ivecs", "200/200"),
                ],
            "2i": {"num_queries": _input.param("2i_queries", 7),
                   "num_indexes": _input.param("2i_indexes", 7)},
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
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`color`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (size, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (brand, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (category, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (review, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "mix_indexes": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (color, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (country, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (type, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries": [
                'SELECT id from {bucket}.{scope}.{collection} where size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where color="Green" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review=1 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review in [1.0, 2.0] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`color`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`brand`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`category`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`type`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                # 'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `embedding` VECTOR, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries_base64": [
                'SELECT id from {bucket}.{scope}.{collection} where size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where color="Green" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review=1 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review in [1.0, 2.0] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
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
            "2i": {"num_queries": _input.param("2i_queries", 5),
                   "num_indexes": _input.param("2i_indexes", 5)},
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
            #     'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR, `review`, `type`, `category`,`country`, `brand`, `color`, `size`, `id`) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
            #     ],
            # "queries": [
            #     'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {bucket}.{scope}.{collection} where color="Green" AND size in [5,6] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {bucket}.{scope}.{collection} where brand="Nike" AND color in ["Green","Red"] AND size in [5,6,7] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {bucket}.{scope}.{collection} where country="USA" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {bucket}.{scope}.{collection} where category="Shoes" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {bucket}.{scope}.{collection} where `type`="Casual" ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {bucket}.{scope}.{collection} where review=1.0 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {bucket}.{scope}.{collection} where review in [1.0, 2.0]ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {bucket}.{scope}.{collection} where review < 6  ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     'SELECT id from {bucket}.{scope}.{collection} where review > 0 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 100',
            #     ],
            "indexes": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `size`, `embedding` VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "bhive_indexes": [
                'CREATE VECTOR INDEX {index_name} ON {bucket}.{scope}.{collection}(`embedding` VECTOR) INCLUDE (`review`, `type`, `category`,`country`, `brand`, `color`, `size`, id) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};'
                ],
            "indexes_base64": [
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`review`, `type`, `category`,`country`, `brand`, `color`, `size`, decode_vector(`embedding`, false) VECTOR, `id`) PARTITION BY HASH(meta().id) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                ],
            "queries": [
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size in [5,6] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color in ["Green","Red"] AND size in [5,6,7] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand in ["Nike","Adidas"] AND color in ["Green","Red", "Blue"] AND size in [5,6,7,8] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country in ["USA","Canada"] AND brand in ["Nike","Adidas", "Puma"] AND color in ["Green","Red", "Blue","Purple"] AND size in [5,6,7,8,9] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category in ["Shoes","Jeans"] AND country in ["USA","Canada","Australia"] AND brand in ["Nike","Adidas","Puma","Asic"] AND color in ["Green","Red", "Blue","Purple","Pink"] AND size in [5,6,7,8,9,10] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type` in ["Casual","Formal"] AND category in ["Shoes","Jeans","Shirt"] AND country in ["USA","Canada","Australia","England"] AND brand in ["Nike","Adidas","Puma","Asic","Brook"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow"] AND size in [5,6,7,8,9,10,11] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review in [1.0, 2.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review in [1.0, 2.0, 5.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review in [1.0, 2.0, 5.0, 10.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                ],
            "queries_base64": [
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size=5 ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color="Green" AND size in [5,6] ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand="Nike" AND color in ["Green","Red"] AND size in [5,6,7] ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country="USA" AND brand in ["Nike","Adidas"] AND color in ["Green","Red", "Blue"] AND size in [5,6,7,8] ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category="Shoes" AND country in ["USA","Canada"] AND brand in ["Nike","Adidas", "Puma"] AND color in ["Green","Red", "Blue","Purple"] AND size in [5,6,7,8,9] ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type`="Casual" AND category in ["Shoes","Jeans"] AND country in ["USA","Canada","Australia"] AND brand in ["Nike","Adidas","Puma","Asic"] AND color in ["Green","Red", "Blue","Purple","Pink"] AND size in [5,6,7,8,9,10] ORDER BY APPROX_VECTOR_DISTANCE(DECODE_VECTOR(embedding,False) , $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review=1.0 AND `type` in ["Casual","Formal"] AND category in ["Shoes","Jeans","Shirt"] AND country in ["USA","Canada","Australia","England"] AND brand in ["Nike","Adidas","Puma","Asic","Brook"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow"] AND size in [5,6,7,8,9,10,11] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review in [1.0, 2.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review in [1.0, 2.0, 5.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
                # 'SELECT id from {bucket}.{scope}.{collection} where review in [1.0, 2.0, 5.0, 10.0] AND `type` in ["Casual","Formal","Sports"] AND category in ["Shoes","Jeans","Shirt","Shorts"] AND country in ["USA","Canada","Australia","England","India"] AND brand in ["Nike","Adidas","Puma","Asic","Brook","Hoka"] AND color in ["Green","Red", "Blue","Purple","Pink","Yellow","Brown"] AND size in [5,6,7,8,9,10,11,12] ORDER BY APPROX_VECTOR_DISTANCE(embedding, $vector, "{similarity}", {nProbe}) limit 10',
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
            "2i": {"num_queries": _input.param("2i_queries", 6),
                   "num_indexes": _input.param("2i_indexes", 1)},
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
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`,`vector` VECTOR) PARTITION BY hash((meta().`id`)) WITH {{"defer_build":true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`free_breakfast`,`type`,`free_parking`,array_count((`public_likes`)),`price`,`country`, vector VECTOR) PARTITION BY HASH (type) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`free_breakfast`,`free_parking`,`country`,`city`, vector VECTOR)  PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`country`, `city`,`price`,`name`, vector VECTOR)  PARTITION BY HASH (country, city) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(vector VECTOR, `price`,`name`,`city`,`country`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`name` INCLUDE MISSING DESC,`phone`,`type`, vector VECTOR) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`city` INCLUDE MISSING ASC, `phone`, vector VECTOR) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};',
                'CREATE INDEX {index_name} ON {bucket}.{scope}.{collection}(`avg_rating`, `price`, `country`, `city`, vector VECTOR) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true, "num_replica":1, {vector}}};'
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
            "2i": {"num_queries": _input.param("2i_queries", 2),
                   "num_indexes": _input.param("2i_indexes", 2)}
            },
        ],
    }