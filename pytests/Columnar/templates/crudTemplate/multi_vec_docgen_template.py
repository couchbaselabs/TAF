"""
Created on 03-Sep-2026

@author: himanshu.jain@couchbase.com
"""

import random
import threading


class MultiVecProduct:
    """
    Generates multi-vector product-style documents for the Columnar
    vector search P2/edge-case tests (pytests/Columnar/vector_search.py).
    Mirrors the doc shape used in the manual data/multiVec.py generator:
    id + embedding field(s) + color/brand/country/category/type/size/
    review/mutate/tags.

    A single class, driven entirely by constructor kwargs, covers every
    doc shape the P2 tests need instead of one subclass per scenario:

    embedding_mode:
      - "normal" (default): embedding_field always holds a valid
        dimension-length vector of ints.
      - "missing": embedding_field is left out of the document with
        probability missing_probability.
      - "null": embedding_field is always JSON null.
      - "heterogeneous": embedding_field holds a random mix of a valid
        vector / int / string / bool / null per document.
      - "variable_dimension": embedding_field holds a valid vector, but
        its length is chosen per-document from variable_dimensions.

    second_embedding_field (optional): when set, adds a second,
    always-valid vector field of second_embedding_dimension length -
    used for the multiple-embedding-fields scenarios.
    """

    COLORS = ["Green", "Black", "White", "Blue", "Red",
              "Grey", "Yellow", "Orange", "Pink", "Brown"]
    BRANDS = ["Nike", "Adidas", "Puma", "Reebok",
              "New Balance", "Under Armour", "Skechers", "Asics"]
    COUNTRIES = ["USA", "Germany", "China", "Vietnam", "Indonesia", "India"]
    TYPES_BY_CATEGORY = {
        "Shoes": ["Casual", "Running", "Basketball", "Training", "Sandals"],
        "Apparel": ["T-Shirt", "Hoodie", "Jacket", "Shorts", "Pants"],
        "Accessories": ["Backpack", "Cap", "Socks", "Bag", "Belt"]
    }
    TAGS_POOL = ["new", "sale", "limited", "bestseller", "eco-friendly",
                 "clearance", "trending", "premium", "exclusive", "outdoor"]

    def __init__(self, embedding_field="embedding", dimension=128,
                 embedding_mode="normal", missing_probability=0.1,
                 variable_dimensions=(128, 256), value_min=0, value_max=255,
                 second_embedding_field=None, second_embedding_dimension=256,
                 include_tags=True, tags_pool=None, min_tags=3, max_tags=5):
        self.embedding_field = embedding_field
        self.dimension = dimension
        self.embedding_mode = embedding_mode
        self.missing_probability = missing_probability
        self.variable_dimensions = variable_dimensions
        self.value_min = value_min
        self.value_max = value_max
        self.second_embedding_field = second_embedding_field
        self.second_embedding_dimension = second_embedding_dimension
        self.include_tags = include_tags
        self.tags_pool = tags_pool or self.TAGS_POOL
        self.min_tags = min_tags
        self.max_tags = max_tags
        self.id = 0
        self._id_lock = threading.Lock()

    def _random_vector(self, dimension):
        return [random.randint(self.value_min, self.value_max)
                for _ in range(dimension)]

    def _embedding_value(self):
        """
        Returns the value to store in embedding_field, or Ellipsis
        (...) to mean "leave the field out of the document".
        """
        if self.embedding_mode == "normal":
            return self._random_vector(self.dimension)
        elif self.embedding_mode == "missing":
            if random.random() < self.missing_probability:
                return ...
            return self._random_vector(self.dimension)
        elif self.embedding_mode == "null":
            return None
        elif self.embedding_mode == "heterogeneous":
            kind = random.choice(["vector", "int", "string", "bool", "null"])
            if kind == "vector":
                return self._random_vector(self.dimension)
            elif kind == "int":
                return random.randint(0, 1000)
            elif kind == "string":
                return random.choice(
                    ["not_a_vector", "invalid", "abc123", ""])
            elif kind == "bool":
                return random.choice([True, False])
            else:
                return None
        elif self.embedding_mode == "variable_dimension":
            return self._random_vector(
                random.choice(self.variable_dimensions))
        else:
            raise ValueError(
                "Unknown embedding_mode={0}".format(self.embedding_mode))

    def generate_document(self):
        with self._id_lock:
            self.id += 1
            doc_id = self.id
        category = random.choice(list(self.TYPES_BY_CATEGORY))
        doc = {
            "id": str(doc_id),
            "size": random.choice(range(5, 13)),
            "color": random.choice(self.COLORS),
            "brand": random.choice(self.BRANDS),
            "country": random.choice(self.COUNTRIES),
            "category": category,
            "type": random.choice(self.TYPES_BY_CATEGORY[category]),
            "review": random.randint(0, 5),
            "mutate": random.randint(0, 1),
        }

        if self.include_tags:
            doc["tags"] = random.sample(
                self.tags_pool,
                k=random.randint(self.min_tags, self.max_tags))

        embedding_value = self._embedding_value()
        if embedding_value is not ...:
            doc[self.embedding_field] = embedding_value

        if self.second_embedding_field:
            doc[self.second_embedding_field] = self._random_vector(
                self.second_embedding_dimension)

        return doc


if __name__ == "__main__":
    import json
    gen = MultiVecProduct()
    docs = [gen.generate_document() for _ in range(10)]
    print(json.dumps(docs, indent=2))
