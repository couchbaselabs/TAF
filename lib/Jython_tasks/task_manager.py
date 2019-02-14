from java.util.concurrent import Callable
from java.util.concurrent import Executors, TimeUnit
from Jython_tasks.shutdown import shutdown_and_await_termination

class TaskManager():
    
    def __init__(self, number_of_threads=10):
        self.number_of_threads = number_of_threads
        self.pool = Executors.newFixedThreadPool(self.number_of_threads)
        self.futures = {}
        self.tasks = []
        
    def add_new_task(self, task):
        future = self.pool.submit(task)
        self.futures[task.thread_name] = future
        self.tasks.append(task)
        
    def get_all_result(self):
        return self.pool.invokeAll(self.tasks)
    
    def get_task_result(self, task):
        future = self.futures[task.thread_name]
        return future.get()

    def stop_task(self, task):
        future = self.futures[task.thread_name]
        future.cancel(True)
    
    def shutdown_task_manager(self, timeout=5):
        shutdown_and_await_termination(self.pool, timeout)
        
        