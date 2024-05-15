from abc import abstractmethod
from sirius_client_framework import sirius_constants


class ExternalStorage:
    def __init__(self, cloud_provider=sirius_constants.SiriusCodes.Providers.AWS):
        self.cloud_provider = cloud_provider

    @abstractmethod
    def get_parameters(self):
        pass


class AwsS3(ExternalStorage):
    def __init__(
            self,
            bucket=None,
            aws_access_key=None,
            aws_secret_key=None,
            aws_session_token=None,
            aws_region=None,
            folder_path=None,
            file_format=None,
            file_path=None,
            num_folders=None,
            folders_per_depth=None,
            files_per_folder=None,
            folder_level_names=None,
            max_folder_depth=None,
            min_file_size=None,
            max_file_size=None):
        super(AwsS3, self).__init__(sirius_constants.SiriusCodes.Providers.AWS)
        self.bucket = bucket
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_session_token = aws_session_token
        self.aws_region = aws_region
        self.folder_path = folder_path
        self.file_format = file_format
        self.file_path = file_path
        self.num_folders = num_folders
        self.folders_per_depth = folders_per_depth
        self.files_per_folder = files_per_folder
        self.folder_level_names = folder_level_names
        self.max_folder_depth = max_folder_depth
        self.min_file_size = min_file_size
        self.max_file_size = max_file_size

    def get_parameters(self):
        parameter_dict = {}
        if self.bucket is not None:
            parameter_dict["bucket"] = self.bucket
        if self.aws_access_key is not None:
            parameter_dict["awsAccessKey"] = self.aws_access_key
        if self.aws_secret_key is not None:
            parameter_dict["awsSecretKey"] = self.aws_secret_key
        if self.aws_session_token is not None:
            parameter_dict["awsSessionToken"] = self.aws_session_token
        if self.aws_region is not None:
            parameter_dict["awsRegion"] = self.aws_region
        if self.folder_path is not None:
            parameter_dict["folderPath"] = self.folder_path
        if self.file_format is not None:
            parameter_dict["fileFormat"] = self.file_format
        if self.file_path is not None:
            parameter_dict["filePath"] = self.file_path
        if self.num_folders is not None:
            parameter_dict["numFolders"] = self.num_folders
        if self.folders_per_depth is not None:
            parameter_dict["foldersPerDepth"] = self.folders_per_depth
        if self.files_per_folder is not None:
            parameter_dict["filesPerFolder"] = self.files_per_folder
        if self.folder_level_names is not None:
            parameter_dict["folderLevelNames"] = self.folder_level_names
        if self.max_folder_depth is not None:
            parameter_dict["maxFolderDepth"] = self.max_folder_depth
        if self.min_file_size is not None:
            parameter_dict["minFileSize"] = self.min_file_size
        if self.max_file_size is not None:
            parameter_dict["maxFileSize"] = self.max_file_size
        return parameter_dict
