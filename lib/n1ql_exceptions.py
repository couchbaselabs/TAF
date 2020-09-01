class N1qlException(object):
    CasMismatchException = "CAS mismatch"
    DocumentExistsException = "document exists"
    DocumentNotFoundException = "document not found"
    DocumentAlreadyExistsException = "Duplicate Key"
    AtrNotFoundException = "atr not found"
    SyncWriteInProgressException = "durable write in progress"