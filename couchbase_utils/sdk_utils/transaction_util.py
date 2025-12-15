from couchbase.collection import Collection
from couchbase.exceptions import (
    TransactionOperationFailed,
    DocumentNotFoundException,
    CouchbaseException,
    TransactionFailed)
from couchbase.logic.n1ql import QueryScanConsistency
from couchbase.options import TransactionOptions

from sdk_utils.sdk_options import SDKOptions


class TransactionLoader(object):
    def __init__(self, collection,
                 create_docs=None, update_docs=None, delete_keys=None,
                 commit_transaction=True, update_count=None,
                 transaction_options=None, is_binary_transaction=False):
        self.collection = collection
        self.create_docs = create_docs or list()
        self.update_docs = update_docs or list()
        self.delete_keys = delete_keys or list()
        self.commit_trans = commit_transaction
        self.update_count = update_count
        self.trans_options = transaction_options
        self.is_binary_transaction = is_binary_transaction
        self.trans_query = None

    @staticmethod
    def get_transaction_options(durability=None, kv_timeout=None,
                                expiration_time=None, scan_consistency=None,
                                metadata_scope=None,
                                metadata_collection=None):
        if durability is not None:
            durability = SDKOptions.get_durability_level(durability)
        if kv_timeout is not None:
            kv_timeout = SDKOptions.get_duration(kv_timeout,
                                                 time_unit="seconds")
        if expiration_time is not None:
            expiration_time = SDKOptions.get_duration(expiration_time,
                                                      time_unit="seconds")
        if scan_consistency is not None:
            scan_consistency = QueryScanConsistency(scan_consistency)
        if metadata_collection is not None:
            metadata_collection = Collection(metadata_scope,
                                             metadata_collection)
        return TransactionOptions(
            durability=durability,
            kv_timeout=kv_timeout,
            expiration_time=expiration_time,
            scan_consistency=scan_consistency,
            metadata_collection=metadata_collection)

    def run_transaction(self, context):
        # Create operation
        for key, doc in self.create_docs:
            context.insert(self.collection, key, doc)

            # Get the same doc back to validate if insert was success
            t_doc = context.get(self.collection, key)
            t_val = t_doc.content_as[dict]
            if t_val != doc:
                raise TransactionOperationFailed("Read doc != Inserted doc")

        # Update operation
        for key, doc in self.update_docs:
            if self.update_count is not None:
                doc["mutated"] = self.update_count
            else:
                doc["mutated"] += 1
            # Get the document to get its CAS value for replace
            t_doc = context.get(self.collection, key)
            # Replace the document with updated content
            context.replace(t_doc, doc)
            # Get again to confirm the change is reflected within the transaction
            t_doc = context.get(self.collection, key)
            t_val = t_doc.content_as[dict]
            if t_val != doc:
                raise TransactionOperationFailed("Read doc != Replaced doc")

        # Delete operation
        for key in self.delete_keys:
            context.remove(key)
            try:
                context.get(key)
            except DocumentNotFoundException:
                pass
            except CouchbaseException as e:
                raise TransactionOperationFailed(
                    f"Invalid exception after doc delete: {e}")

        if not self.commit_trans:
            raise TransactionFailed("User rollback requested")
