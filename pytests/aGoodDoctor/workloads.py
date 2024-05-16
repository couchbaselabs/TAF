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
    "2iQPS": 10,
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
            "cbas": [0, 0, 0]
            },
        {
            "valType": "NimbusM",
            "2i": [1, 2],
            "FTS": [0, 0],
            "cbas": [0, 0, 0]
            }
        ]
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
