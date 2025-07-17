"""
Created on 14-July-2025

@author: himanshu.jain@couchbase.com
"""

import json


class Column:
    def __init__(self, no_of_columns=1, no_of_levels=0, sparse=False, allArrays=False):
        self.no_of_columns = no_of_columns
        self.no_of_levels = no_of_levels
        self.sparse = sparse
        self.allArrays = allArrays
        self.id = 0
        self.column_id = 0
        self.rare_column_count = 3

    def generate_document(self):

        def get_round_robin_value(index):
            column_type = index % 5  # Round-robin from 0 to 4

            if column_type == 0:  # BOOLEAN
                return index % 2 == 0
            elif column_type == 1:  # STRING
                return f"value{index+1}"
            elif column_type == 2:  # BIGINT
                return 10
            elif column_type == 3:  # DOUBLE
                return float(3.14)
            elif column_type == 4:  # NULL
                return None
            else:
                raise ValueError(f"Invalid column type: {column_type}")

        def build_doc(doc_id, level):
            doc = {"id": str(doc_id)}

            doc["commonColumn"] = "commonValue"

            if self.sparse and self.rare_column_count > 0:
                self.rare_column_count -= 1
                # random.choice(["rareValue", None, False, ["rareValue", 1.1]])
                doc["rareColumn"] = 15

            for _ in range(self.no_of_columns):
                value = get_round_robin_value(self.column_id)
                if self.allArrays:
                    doc[f"column{self.column_id+1}"] = [value]
                else:
                    doc[f"column{self.column_id+1}"] = value
                self.column_id += 1

            if level <= self.no_of_levels:
                doc[f"level{level}"] = build_doc(doc_id, level + 1)
            return doc

        self.id += 1
        doc = build_doc(self.id, 1)
        return doc


if __name__ == "__main__":
    column = Column(no_of_columns=1000, no_of_levels=0, sparse=True)
    res = []
    for i in range(1000):
        doc = column.generate_document()
        res.append(doc)
    print(json.dumps(res, indent=2))
