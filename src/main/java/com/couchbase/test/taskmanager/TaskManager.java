package com.couchbase.test.taskmanager;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TaskManager {
    private int workers;
    private ExecutorService poolExecutor;
    private ConcurrentHashMap<String, Future> tasks = new ConcurrentHashMap<String, Future>();

    public TaskManager(int workers) {
        this.workers = workers;
        this.poolExecutor = Executors.newFixedThreadPool(this.workers);
    }

    public void shutdown() {
        this.poolExecutor.shutdown();
    }

    public void submit(Task task) {
        Future future = this.poolExecutor.submit(task);
        this.tasks.put(task.taskName, future);
    }

    public void getAllTaskResult() {
        for (String taskName : this.tasks.keySet()) {
            try {
                this.tasks.get(taskName).get();
                this.tasks.remove(taskName);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }    
        }
    }

    public boolean getTaskResult(Task task) {
        try {
            this.tasks.get(task.taskName).get();
            this.tasks.remove(task.taskName);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return task.result;
    }

    public void abortTask(Task task) {
        this.tasks.get(task.taskName).cancel(true);
    }

    public void abortAllTasks() {
        for (Entry<String, Future> task : this.tasks.entrySet()) {
            this.tasks.get(task.getKey()).cancel(true);
        }
    }
}