package edu.boun.edgecloudsim.statistic;

import edu.boun.edgecloudsim.task_generator.Task;

import java.util.ArrayList;
import java.util.List;

public class Data {
    public static List<Task> finishedTasks = new ArrayList<Task>();

    public static void addFinishedTasks(Task task){
        finishedTasks.add(task);
    }

    public static double overallDelay(){
        double sum = 0;
        for( Task task : finishedTasks){
            sum += task.finishTime - task.arrivalTime;

        }
        return sum/finishedTasks.size() ;
    }

    public static int getFinishedTaskSum(){
        return finishedTasks.size();
    }
}
