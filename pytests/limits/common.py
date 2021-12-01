import json
import string
import random


def create_document_of_size(throughput, key="key"):
    # The bytes we subtract from the number of "a"s that are sent
    difference = len(json.dumps({key: ""}, separators=(',', ':')))
    # Returns a document with the corresponding number of 'a's to make the
    # document
    return {key: "a" * max(0, throughput - difference)}


def random_string(length):
    return ''.join(random.choice(string.lowercase) for _ in range(length))
