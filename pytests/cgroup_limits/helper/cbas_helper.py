import time
from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI


class CGroupCBASHelper:

    def __init__(self, cluster, logger):
        self.cluster = cluster
        self.log = logger
        self.cbas_node = self.cluster.cbas_nodes[0]
        self.log.info("CBAS node: {}".format(self.cbas_node.__dict__))

    def create_cbas_functions(self):

        self.dataset_count = 0
        cbas_rest = AnalyticsRestAPI(self.cbas_node)
        dataverse_statement = "CREATE DATAVERSE mydataverse IF NOT EXISTS;"
        self.log.info("Running query on cbas: {}".format(dataverse_statement))
        status, content = cbas_rest.execute_statement_on_cbas(dataverse_statement)
        self.log.info("Status = {}, Content = {}".format(status, content))
        for bucket in self.cluster.buckets:
            dataset_name = "mydataset" + str(self.dataset_count)
            statement = "USE mydataverse; CREATE DATASET {} ON `{}`." \
                "`_default`.`_default`;".format(dataset_name, bucket.name)
            self.log.info("Running query on cbas: {}".format(statement))
            status, content = cbas_rest.execute_statement_on_cbas(statement)
            self.log.info(f"Status = {status}, Content = {content}")
            self.dataset_count += 1
        connect_stmt = "CONNECT LINK Local;"
        self.log.info("Running query on cbas: {}".format(connect_stmt))
        status, content = cbas_rest.execute_statement_on_cbas(connect_stmt)
        self.log.info(f"Status = {status}, Content = {content}")

        time.sleep(20)

        statement = 'USE mydataverse; SELECT VALUE d.DataverseName || "." ' \
                    '|| d.DatasetName FROM Metadata.`Dataset` d ' \
                    'WHERE d.DataverseName = "mydataverse";'
        self.log.info("Running query on cbas: {}".format(statement))
        status, content = cbas_rest.execute_statement_on_cbas(statement)
        self.log.info(f"Status = {status}, Content = {content}")