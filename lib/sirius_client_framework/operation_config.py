from abc import abstractmethod, ABCMeta


class OperationConfig:
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def get_parameters(self):
        pass


class WorkloadOperationConfig(OperationConfig):
    def __init__(
            self,
            start=None,
            end=None,
            template=None,
            doc_size=None,
            fields_to_change=None,
    ):
        """
            :param start: start of range for doc generation (int, optional)
            :param end: end of range for doc generation (int, optional)
            :param template: template for doc generation (string, optional)
            :param doc_size: size of docs to be generated (int, optional)
            :param fields_to_change: list of fields to be updated (list, optional)
        """
        super(WorkloadOperationConfig, self).__init__()
        self.start = start
        self.end = end
        self.template = template
        self.doc_size = doc_size
        self.fields_to_change = fields_to_change

    def get_parameters(self):
        parameter_dict = {}
        if self.start is not None:
            parameter_dict['start'] = self.start
        if self.end is not None:
            parameter_dict['end'] = self.end
        if self.doc_size is not None:
            parameter_dict['docSize'] = self.doc_size
        if self.template is not None:
            parameter_dict['template'] = self.template
        if self.fields_to_change is not None:
            parameter_dict['fieldsToChange'] = self.fields_to_change
        return parameter_dict
