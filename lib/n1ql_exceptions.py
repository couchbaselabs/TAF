class N1qlException(object):
    CasMismatchException = "CAS mismatch"
    DocumentExistsException = "document already exists"
    DocumentNotFoundException = "document not found"
    DocumentAlreadyExistsException = "Duplicate Key"
    AtrNotFoundException = "atr not found"
    SyncWriteInProgressException = "durable write in progress"
    MemoryQuotaError = "Request has exceeded memory quota"