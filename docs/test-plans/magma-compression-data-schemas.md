# Magma Compression — DocLoader Data Schemas

**Test file:** `pytests/storage/magma/magma_compression_benchmarking.py`
**Conf file:** `conf/magma/magma_benchmark_compression.conf`
**Schema source:** `DocLoader/src/main/java/utils/val/`

The `value_type` parameter controls which document schema the Sirius Java DocLoader generates.
It is passed directly to `SiriusCouchbaseLoader` and routed through the DocLoader REST API to
`DocumentGenerator.java`, which selects the corresponding Java class.

---

## Schemas Used in Compression Testing

### 1. SimpleValue *(default)*

**Parameter:** `value_type=SimpleValue`
**Source:** `DocLoader/.../utils/val/SimpleValue.java`

**Structure:**
```json
{
  "name": "<100-char string>",
  "age": 42,
  "animals": ["red", "blue"],
  "attributes": {
    "colour": "green",
    "dimensions": { "width": 10, "height": 20 },
    "hobbies": [{ "hobby1": "reading", "hobby2": "cycling",
                  "details": { "location": { "lat": 12.5, "lon": 77.2 } } }]
  },
  "gender": "male",
  "marital_status": "single",
  "mutated": 0,
  "body": "<padding string to reach doc_size>"
}
```

**Compressibility:** Very high (~80% savings with lz4/zstd)

**Why:** The `body` field is a seeded random string that repeats itself to pad the document to
`doc_size` bytes. Because the same character sequence repeats, the compressor sees long runs
of identical substrings and achieves very high compression ratios. This is the best-case scenario.

**Use in testing:** Proves the compression algorithm works at all. The iter-1 hard assert
(`net_savings_gb > 0`) is most strict with this schema — if it fails here, the algorithm is broken.

---

### 2. Hotel

**Parameter:** `value_type=Hotel`
**Source:** `DocLoader/.../utils/val/Hotel.java`

**Structure:**
```json
{
  "free_breakfast": true,
  "free_parking": false,
  "phone": "+1-555-0123",
  "name": "John Smith",
  "price": 1200,
  "avg_rating": 3.7,
  "address": "123 Main St",
  "city": "Austin",
  "country": "United States",
  "email": "john.smith@hotels.com",
  "public_likes": ["Alice Brown", "Bob Jones"],
  "reviews": [
    { "author": "...", "date": "2024-01-15 10:00:00",
      "ratings": { "Cleanliness": 4, "Overall": 3, "Rooms": 5, "Value": 2 } }
  ],
  "type": "Hotel",
  "url": "http://example.com",
  "mutate": 0,
  "counter": 1001
}
```

**Compressibility:** High (~50-60% savings)

**Why:** All field names repeat across every document (strong compression). Values come from
Faker-generated realistic data (addresses, names, URLs) — natural language has repetitive
patterns that compressors exploit. Reviews array adds nested structure with repeated keys.

**Use in testing:** Real-world production workload simulation. Validates compression on the
kind of data actual Couchbase customers store.

---

### 3. Product

**Parameter:** `value_type=Product`
**Source:** `DocLoader/.../utils/val/Product.java`

**Structure:**
```json
{
  "id": "Pkey_0001",
  "product_name": "Alice",
  "product_link": "http://example.com/p/123",
  "product_features": ["Fiction", "Mystery"],
  "product_specs": { "make": "Author Name", "model": "Book Title" },
  "product_image_links": ["http://img1.com", "http://img2.com"],
  "product_reviews": [
    { "author": "...", "date": "...",
      "ratings": { "rating_value": 7, "performance": 8,
                   "utility": 5, "pricing": 3, "build_quality": 9 } }
  ],
  "product_category": ["Romance", "Thriller"],
  "price": 4999.50,
  "upload_date": "Mon Jan 15 ...",
  "avg_rating": 4.2,
  "num_sold": "32000",
  "weight": 250.5,
  "quantity": 1500,
  "seller_name": "Pale Ale",
  "seller_location": "Chicago , USA",
  "seller_verified": true,
  "template_name": "Product",
  "mutated": 0
}
```

**Compressibility:** Medium (~30-40% savings)

**Why:** Intentionally mixes types — `num_sold`, `weight`, and `quantity` are randomly stored
as either numeric or string on each update. This type variance reduces compression efficiency
compared to Hotel. Floats and doubles have low compressibility. Realistic but harder to compress.

**Use in testing:** Validates compression on heterogeneous/mixed-type schemas — stress-tests
whether the algorithm degrades gracefully when field types vary between documents.

---

### 4. RandomlyNestedJson

**Parameter:** `value_type=RandomlyNestedJson`
**Source:** `DocLoader/.../utils/val/RandomlyNestedJson.java`

**Structure:** (example — varies per document)
```json
{
  "key_0": {
    "key_1": { "key_0": 742, "key_2": true },
    "key_3": "value_58"
  },
  "key_2": 3.141592,
  "key_3": ["array_value_12", 99, false],
  "key_4": null
}
```

**Compressibility:** Very low (~5-10% savings)

**Why:** Every document has a randomly chosen nesting depth (up to 5 levels), randomly chosen
number of keys per object (1-5), and fully random primitive values (int, float, double, bool,
string, null, nested array). No two documents share the same structure or values. The compressor
finds almost no repeated patterns — this is the worst-case scenario.

**Use in testing:** Validates that fragmentation overhead does NOT get falsely masked when
compression savings are minimal. The iter-1 assert may fail if `compression_data_algo=none`
is combined with this schema — which is the correct and expected outcome.

---

## Coverage Summary

| Schema | `value_type` | Compressibility | Purpose |
|---|---|---|---|
| SimpleValue | `SimpleValue` | ~80% | Best case — proves algorithm works |
| Hotel | `Hotel` | ~50-60% | Real-world structured data |
| Product | `Product` | ~30-40% | Mixed-type heterogeneous schema |
| RandomlyNestedJson | `RandomlyNestedJson` | ~5-10% | Worst case — near-incompressible |

These four schemas provide a full spectrum from best to worst compressibility, ensuring the
compression validation covers the entire range of real-world and adversarial workloads.

---

## How to Run

Pass `value_type` as a test parameter alongside the compression algorithm:

```
# Best case
storage.magma.magma_compression.MagmaCompressionTests.test_disk_usage_vs_fragmentation,\
  compression_data_algo=lz4,num_iterations=3,value_type=SimpleValue,\
  nodes_init=1,num_items=10000000,load_docs_using=sirius_java_sdk,init_loading=False

# Real-world
storage.magma.magma_compression.MagmaCompressionTests.test_disk_usage_vs_fragmentation,\
  compression_data_algo=lz4,num_iterations=3,value_type=Hotel,\
  nodes_init=1,num_items=10000000,load_docs_using=sirius_java_sdk,init_loading=False

# Mixed-type
storage.magma.magma_compression.MagmaCompressionTests.test_disk_usage_vs_fragmentation,\
  compression_data_algo=lz4,num_iterations=3,value_type=Product,\
  nodes_init=1,num_items=10000000,load_docs_using=sirius_java_sdk,init_loading=False

# Worst case
storage.magma.magma_compression.MagmaCompressionTests.test_disk_usage_vs_fragmentation,\
  compression_data_algo=lz4,num_iterations=3,value_type=RandomlyNestedJson,\
  nodes_init=1,num_items=10000000,load_docs_using=sirius_java_sdk,init_loading=False
```

The default (`value_type=SimpleValue`) is used when `value_type` is not specified.
