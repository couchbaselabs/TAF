import json
import time
from global_vars import logger
from abc import ABCMeta, abstractmethod
from backup_service_client.models.task_template import TaskTemplate
from backup_service_client.models.task_template_schedule import TaskTemplateSchedule
from backup_service_client.models.task_template_merge_options import TaskTemplateMergeOptions
from backup_service_client.configuration import Configuration
from backup_service_client.api_client import ApiClient
from backup_service_client.api.plan_api import PlanApi
from backup_service_client.api.import_api import ImportApi
from backup_service_client.api.repository_api import RepositoryApi
from backup_service_client.api.configuration_api import ConfigurationApi
from backup_service_client.api.active_repository_api import ActiveRepositoryApi
from backup_service_client.models.plan import Plan
from backup_service_client.models.archive_request import ArchiveRequest
from backup_service_client.models.create_active_repository_request import CreateActiveRepositoryRequest
from nfs import NfsConnection
from membase.api.rest_client import RestHelper, RestConnection
from threading import Timer

log = logger.get("test")


class BackupTasks:

    def __init__(self, backup_service):
        self.backup_service = backup_service

    def create_predefined_plans(self):
        """ Loads all predefined plans defined in `PrefinedPlans` """
        for plan in PredefinedPlans.plans.values():
            self.backup_service.api.create_plan(plan)

        plans = set(plan.name for plan in self.backup_service.api.get_plans())

        log.info("Created {}".format(plans))

        for plan in PredefinedPlans.plans.values():
            assert plan.name in plans

    def create_predefined_repos(self):
        """ Creates a repository for each of the predefined plans """
        for plan in PredefinedPlans.plans.values():
            self.backup_service.api.create_repository("repo-{}".format(plan.name), plan.name,
                                                      "{}/archive-{}".format(self.backup_service.directory_to_mount, plan.name))

        repos = set(repo.id for repo in self.backup_service.api.get_repositories('active'))

        log.info("Created {}".format(repos))

        for plan in PredefinedPlans.plans.values():
            assert "repo-{}".format(plan.name) in repos


class BackupService:

    def __init__(self, servers, primary_server=None):
        # Pick the primary server to communicate with
        self.primary_server = primary_server if primary_server else servers[0]

        # The backup service API
        self.api = BackupServiceAPI(self.primary_server)

        # Share and mount folder
        self.directory_to_share = "/tmp/share"
        self.directory_to_mount = "/tmp/my_archive"
        self.nfs = NfsConnection(servers[0], servers, self.directory_to_share, self.directory_to_mount)

    def setup(self):
        self.nfs.share()

    def clean(self):
        self.api.backup_service_cleanup()
        self.nfs.clean()

    def close(self):
        self.nfs.close()


class BackupServiceAPI:

    def __init__(self, server):
        """ The constructor

        Args:
            server (TestInputServer): The server to communicate with.
        """
        self.server = server

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

        # Backup Service Constants
        self.default_plans = ["_hourly_backups", "_daily_backups"]

    def backup_service_cleanup(self):
        """ Delete all repos and plans if the backup service is running
        """
        if self.is_backup_service_running():
            self.delete_all_repositories()
            self.delete_all_plans()

    def is_backup_service_running(self):
        """ Returns true if the backup service is running.
        """
        rest = RestConnection(self.server)
        return 'backupAPI' in json.loads(rest._http_request(rest.baseUrl + "pools/default/nodeServices")[1])['nodesExt'][0]['services'].keys()

    def delete_all_plans(self):
        """ Deletes all plans.

        Deletes all plans using the Rest API with the exceptions of the default plans.
        """
        for plan in self.plan_api.plan_get():
            if plan.name not in self.default_plans:
                self.plan_api.plan_name_delete(plan.name)

    def delete_all_repositories(self):
        """ Deletes all repositories.

        Pauses and Archives all repos and then deletes all the repos using the Rest API.
        """
        # Pause repositories
        for repo in self.repository_api.cluster_self_repository_state_get('active'):
            self.active_repository_api.cluster_self_repository_active_id_pause_post_with_http_info(repo.id)

        # Sleep to ensure repositories are paused
        time.sleep(5)

        # Delete all running tasks
        self.delete_all_running_tasks()

        # Archive repositories
        for repo in self.repository_api.cluster_self_repository_state_get('active'):
            self.active_repository_api.cluster_self_repository_active_id_archive_post_with_http_info(
                repo.id, body=ArchiveRequest(id=repo.id))

        # Delete archived repositories
        for repo in self.repository_api.cluster_self_repository_state_get('archived'):
            self.repository_api.cluster_self_repository_state_id_delete_with_http_info('archived', repo.id)

        # delete imported repositories
        for repo in self.repository_api.cluster_self_repository_state_get('imported'):
            self.repository_api.cluster_self_repository_state_id_delete_with_http_info('imported', repo.id)

    def delete_task(self, state, repo_id, task_category, task_name):
        """ Delete a task
        """
        rest = RestConnection(self.server)
        assert(task_category in ['one-off', 'scheduled'])
        status, content, header = rest._http_request(
            rest.baseUrl + "_p/backup/internal/v1/cluster/self/repository/{}/{}/task/{}/{}".format(state, repo_id, task_category, task_name), 'DELETE')

    def delete_all_running_tasks(self):
        """ Delete all one off and schedule tasks for every repository
        """
        for repo in self.repository_api.cluster_self_repository_state_get('active'):
            repository = self.repository_api.cluster_self_repository_state_id_get('active', repo.id)
            if repository.running_one_off:
                for key, task in repository.running_one_off.items():
                    self.delete_task('active', repo.id, 'one-off', task.task_name)
            if repository.running_tasks:
                for key, task in repository.running_tasks.items():
                    self.delete_task('active', repo.id, 'scheduled', task.task_name)

    def get_plans(self):
        """ Gets all plans """
        return self.plan_api.plan_get()

    def create_plan(self, plan):
        """ Creates a plan

        Attr:
            plan_name (str): The name of the new plan.

        """
        return self.plan_api.plan_name_post(plan.name, body=plan)

    def create_repository(self, repo_name, plan_name, archive_name):
        """ Creates an active repository

        Creates an active repository with a filesystem archive.

        Attr:
            repo_name (str): The name of the new repository.
            plan_name (str): The name of the plan to attach.
            archive_name (str): The name of the archive.
        """
        body = CreateActiveRepositoryRequest(plan=plan_name, archive=archive_name)
        # Add repositories and tie plan to repository
        return self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

    def get_repositories(self, state):
        return self.repository_api.cluster_self_repository_state_get(state)

    def get_task_history(self, state, repo_name, task_name=None):
        if task_name:
            return self.repository_api.cluster_self_repository_state_id_task_history_get(state, repo_name, task_name=task_name)
        return self.repository_api.cluster_self_repository_state_id_task_history_get(state, repo_name)


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


class Recipe:
    """ A class for creating backup service objects given a description """

    @staticmethod
    def make_plan(plan_name, schedule, merge_map=None):
        """ Creates a plan with a schedule and attaches it to the repository

        Attr:
            plan_name (str): The name of the plan.
            schedule (list): A list of tuples of the format [(frequency, period, at_time), ..]
            merge_map (dict): A dict of the format {index: (start_offset, end_offset), } where the tuple can be None.
            Specifies the element at that particular index is a merge task with offsets (start_offset, end_offset).
        """
        if not merge_map:
            merge_map = {}

        def get_task_type(i):
            return "MERGE" if i in merge_map else "BACKUP"

        def get_merge_options(i):
            merge_options = merge_map.get(i, None)

            if merge_options:
                return TaskTemplateMergeOptions(offset_start=merge_options[0], offset_end=merge_options[1])

            return merge_options

        return Plan(name=plan_name, tasks=[TaskTemplate(name="task{}".format(i), task_type=get_task_type(i), schedule=TaskTemplateSchedule(job_type=get_task_type(i),
                                                                                                                                           frequency=freq, period=period, time=at_time), merge_options=get_merge_options(i)) for i, (freq, period, at_time) in enumerate(schedule)])


class PredefinedPlans:
    """ Predefined backup service plans that will be created in the backup service """

    plans = \
        {
            "simple_plan": Recipe.make_plan("simple_plan", [(10, 'MINUTES', None), (35, 'MINUTES', None)], {1: (0, 1)})
        }
