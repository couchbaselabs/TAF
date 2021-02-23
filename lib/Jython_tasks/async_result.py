class AsyncResult:
    """ Bundle a Task with the TaskManager that scheduled that Task so you give
    the calling code the ability to get the results later without giving the
    calling code access to the TaskManager that scheduled the Task.

    Usage:
    >>> task = GenericLoadingTask(cluster, bucket, client)
    >>> self.pool.add_new_task(task)
    >>> async_result = AsyncResult(self.pool, task)
    ...
    >>> result = async_result.get()
    """

    def __init__(self, task_manager, task):
        """ Constructor

        Args:
            task_manager (TaskManager): The TaskManager that scheduled `task`.
            task (Task): The Task that was scheduled by `task_manager`.
        """
        self.task_manager, self.task = task_manager, task

    def get(self):
        """ Return the result when it arrives. """
        return self.task_manager.get_task_result(self.task)

    def ready(self):
        """ Returns when `task` has completed. """
        return self.task.complete

    def successful(self):
        """ Returns True if the `task` was completed without raising an exception.

        Raises a `ValueError` if the result is not ready.
        """
        if not self.ready():
            raise ValueError()

        return self.task.exception is not None
