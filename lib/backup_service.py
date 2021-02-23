from abc import ABCMeta, abstractmethod
from backup_service_client.configuration import Configuration
from backup_service_client.api_client import ApiClient
from backup_service_client.api.plan_api import PlanApi
from backup_service_client.api.import_api import ImportApi
from backup_service_client.api.repository_api import RepositoryApi
from backup_service_client.api.configuration_api import ConfigurationApi
from backup_service_client.api.active_repository_api import ActiveRepositoryApi


class BackupService:

    def __init__(self, server):
        """ The constructor

        Args:
            server (TestInputServer): The server to communicate with.
        """
        # Select a configuration factory which creates Configuration objects that communicate over http
        self.configuration_factory = HttpConfigurationFactory(server)

        # Rest API Configuration
        self.configuration = self.configuration_factory.create_configuration()

        # Create Api Client
        self.api_client = ApiClient(self.configuration)

        # Rest API Sub-APIs
        self.plan_api = PlanApi(self.api_client)
        self.import_api = ImportApi(self.api_client)
        self.repository_api = RepositoryApi(self.api_client)
        self.configuration_api = ConfigurationApi(self.api_client)
        self.active_repository_api = ActiveRepositoryApi(self.api_client)


class AbstractConfigurationFactory:
    __metaclass__ = ABCMeta

    def __init__(self, server, hints=None):
        self.hints, self.server = hints, server

    def create_configuration_common(self):
        """ Creates a configuration and sets its credentials
        """
        configuration = Configuration()

        configuration.username = self.server.rest_username
        configuration.password = self.server.rest_password

        return configuration

    @abstractmethod
    def create_configuration(self):
        """ Creates a Configuration object
        """
        raise NotImplementedError("Please Implement this method")


class HttpConfigurationFactory(AbstractConfigurationFactory):

    def create_configuration(self):
        """ Creates a http configuration object.
        """
        configuration = self.create_configuration_common()

        configuration.host = "http://{}:8091/_p/backup/api/v1".format(self.server.ip)

        return configuration
