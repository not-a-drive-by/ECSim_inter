package edu.boun.edgecloudsim.edge_client;

import edu.boun.edgecloudsim.task_generator.Task;

import java.util.ArrayList;
import java.util.List;

public class Queue {
    private List<Task> taskQueues = new ArrayList<Task>();
    private int quota;

    //无聊函数
    public List<Task> getTaskQueues() {    return taskQueues;   }
    public int getQuota() {  return quota;   }
    public void setQuota(int quota) {   this.quota = quota;    }
    public int getQueueLength(){
        return taskQueues.size();
    }

    @Override
    public String toString() {
        return "Queue{" +
                "taskQueues=" + taskQueues +
                ", quota=" + quota +
                '}';
    }
}
