from collections_helper.collections_spec_constants import MetaCrudParams
from constants.sdk_constants.java_client import SDKConstants

spec = {
    MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 1,
    MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 1,
    MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 1,
    MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
    MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
    MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",
    "doc_crud": {
        MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",
        MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS: 120,
        MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 100,
        MetaCrudParams.DocCrud.DOC_KEY_SIZE: 20,
        MetaCrudParams.DocCrud.DOC_SIZE: 20971520
    },
    MetaCrudParams.DURABILITY_LEVEL: SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY,
}