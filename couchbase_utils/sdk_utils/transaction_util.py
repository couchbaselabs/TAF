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
    def get_transaction_options(durability=None,
                                timeout=None, scan_consistency=None,
                                metadata_scope=None,
                                metadata_collection=None):
        """
        Build TransactionOptions passing only the options that are set.
        'timeout' is the current SDK name for the deprecated
        'expiration_time' option.
        """
        options = dict()
        if durability is not None:
            options["durability"] = SDKOptions.get_durability_level(durability)
        if timeout is not None:
            options["timeout"] = SDKOptions.get_duration(timeout,
                                                         time_unit="seconds")
        if scan_consistency is not None:
            options["scan_consistency"] = QueryScanConsistency(scan_consistency)
        if metadata_collection is not None:
            options["metadata_collection"] = Collection(metadata_scope,
                                                        metadata_collection)
        return TransactionOptions(**options)

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
            # Get the document first to obtain TransactionGetResult
            t_doc = context.get(self.collection, key)
            context.remove(t_doc)
            try:
                context.get(self.collection, key)
            except DocumentNotFoundException:
                pass
            except CouchbaseException as e:
                raise TransactionOperationFailed(
                    f"Invalid exception after doc delete: {e}")

        if not self.commit_trans:
            raise TransactionFailed("User rollback requested")
