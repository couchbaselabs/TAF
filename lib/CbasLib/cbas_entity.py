from TestInput import TestInputSingleton

runtype = TestInputSingleton.input.param("runtype", "default").lower()
if runtype in ["columnar", "columnar1"]:
    from CbasLib.cbas_entity_columnar import (
        Database as database,
        Dataverse as dataverse,
        CBAS_Scope as cbas_scope,
        Link as link,
        Remote_Link as remote_link,
        External_Link as external_link,
        Kafka_Link as kafka_link,
        Dataset as dataset,
        CBAS_Collection as cbas_collection,
        Remote_Dataset as remote_dataset,
        External_Dataset as external_dataset,
        Standalone_Dataset as standalone_dataset,
        Synonym as synonym,
        CBAS_Index as index,
        CBAS_UDF as udf,
        ExternalDB as externaldb)
else:
    from CbasLib.cbas_entity_on_prem import (
        Database as database,
        Dataverse as dataverse,
        CBAS_Scope as cbas_scope,
        Link as link,
        Remote_Link as remote_link,
        External_Link as external_link,
        Kafka_Link as kafka_link,
        Dataset as dataset,
        CBAS_Collection as cbas_collection,
        Remote_Dataset as remote_dataset,
        External_Dataset as external_dataset,
        Standalone_Dataset as standalone_dataset,
        Synonym as synonym,
        CBAS_Index as index,
        CBAS_UDF as udf,
        ExternalDB as externaldb)


class Database(database):
    pass


class Dataverse(dataverse):
    pass


class CBAS_Scope(cbas_scope):
    pass


class Link(link):
    pass


class Remote_Link(remote_link):
    pass


class External_Link(external_link):
    pass


class Kafka_Link(kafka_link):
    pass


class Dataset(dataset):
    pass


class CBAS_Collection(cbas_collection):
    pass


class Remote_Dataset(remote_dataset):
    pass


class External_Dataset(external_dataset):
    pass


class Standalone_Dataset(standalone_dataset):
    pass


class Synonym(synonym):
    pass


class CBAS_Index(index):
    pass


class CBAS_UDF(udf):
    pass


class ExternalDB(externaldb):
    pass
