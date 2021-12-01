from abc import abstractmethod, ABCMeta


class ResourceTask(object):
    __metaclass__ = ABCMeta

    """ Represents throughput for a resource """

    def __init__(self):
        self.throughput = 0

    def set_throughput(self, throughput):
        """ Set the throughput for this particular resource """
        # Ensure the caller does not set it back to the same value
        if self.throughput == throughput:
            raise ValueError("Updated the throughput to the same value")

        # Call methods updating the throughput
        if throughput > self.throughput:
            self.on_throughput_increase(throughput)
        else:
            self.on_throughput_decrease(throughput)

        self.throughput = throughput

    def get_throughput(self):
        """ Returns the throughput for this particular resource """
        return self.throughput

    @abstractmethod
    def on_throughput_increase(self, throughput):
        """ Called when the throughput is updated. """
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def on_throughput_decrease(self, throughput):
        """ Called when the throughput is updated. """
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def get_throughput_success(self):
        """ The throughput that succeeded """
        raise NotImplementedError("Please implement this method.")
    
    def error(self):
        """ Generates an above threshold error """
        raise NotImplementedError("Please implement this method.")

    def expected_error(self):
        """ Returns the expected error message """
        raise NotImplementedError("Please implement this method.")


class UserResourceTask(ResourceTask):
    """ Produces throughput of a resource for a specific user and node """

    def __init__(self, user, node):
        super(UserResourceTask, self).__init__()
        self.user = user
        self.node = node


class ScopeResourceTask(ResourceTask):
    """ Targets throughput of a resource for a specific scope """

    def __init__(self, bucket, scope, user, node):
        super(ScopeResourceTask, self).__init__()
        self.bucket, self.scope, self.user, self.node = bucket, scope, user, node
        self.scope.acquire(self)

    def set_throughput(self, throughput):
        """ Set the throughput for this particular resource """
        # Divide evenly between holders as resource is shared
        throughput = throughput / self.scope.no_of_holders()

        # Last holder gets the remainder
        if self.scope.is_last_holder(self):
            throughput += throughput % self.scope.no_of_holders()
        
        super(ScopeResourceTask, self).set_throughput(throughput)

    def get_throughput(self):
        """ Returns the shared throughput """
        return sum(task.throughput for task in self.scope.held)
