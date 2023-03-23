/*
* 一个DataCenter里的任务调度
* 节点内李雅普诺夫
**/

package edu.boun.edgecloudsim.edge_server;

import edu.boun.edgecloudsim.edge_client.Queue;
import edu.boun.edgecloudsim.statistic.Data;
import edu.boun.edgecloudsim.task_generator.Task;
import edu.boun.edgecloudsim.utils.StaticfinalTags;
import ilog.concert.IloException;
import org.cloudbus.cloudsim.Storage;
import org.w3c.dom.Element;
import java.util.*;
import ilog.concert.*;
import ilog.cplex.IloCplex;

public class EdgeDataCenter {

    //自身属性
    private int id;
    public int CPU;
    public int RAM;
    public int storage;
    private double x_pos;
    private double y_pos;
    private int quota;

    public int totalNum = 0;

    private List<Task> queue1 = new ArrayList<Task>();
    private List<Task> queue2 = new ArrayList<Task>();
    private List<Task> queue3 = new ArrayList<Task>();
    private List<List<Task>> queue = new ArrayList<List<Task>>();

    private List<Task> receiveReqFromTasks = new ArrayList<Task>();
    private List<EdgeVM> activeVM = new ArrayList<EdgeVM>();

    //比较器实例
    TaskPreferenceComparator taskPreferenceComparator = new TaskPreferenceComparator();
    TaskLengthComparator taskLengthComparator = new TaskLengthComparator();

    public EdgeDataCenter(int _id, int _CPU, int _RAM, int _storage, double x, double y){
        this.id = _id;
        this.CPU = _CPU;
        this.RAM = _RAM;
        this.storage = _storage;
        this.x_pos = x;
        this.y_pos = y;

        this.queue.add(queue1);
        this.queue.add(queue2);
        this.queue.add(queue3);

        activeVM.add(new EdgeVM(2, 32,1690));
        activeVM.add(new EdgeVM(2, 30,420));
        activeVM.add(new EdgeVM(2, 7,1690));
    }



    public void updateServerQuota(){
        int sum = 5;
        for( EdgeVM vm : activeVM ){
            if(vm.getProcessingTask() == null){
                sum--;
            }
        }
        quota = sum;
    }

    //更新偏好序列
    public boolean updatePreference(){
        Collections.sort(receiveReqFromTasks, taskPreferenceComparator);
        List<Task> rejectedReqTask;
        //超过限额部分的任务要拒绝
        //被拒绝的任务目标服务器应当清空 发起请求的时候已经从preferenceList中删掉了
        if( receiveReqFromTasks.size() > quota){
            rejectedReqTask = receiveReqFromTasks.subList(quota, receiveReqFromTasks.size());
            receiveReqFromTasks = receiveReqFromTasks.subList(0, quota);
            for(Task t : rejectedReqTask){
                t.setTargetServer(null);
            }
            return true;
        }
        return false;
    }



    //接受卸载的任务
    public void receiveOffloadTasks(Task task){
        int taskType = task.getType();
        if( taskType == 1 ){
            queue1.add(task);
        }else if( taskType == 2 ){
            queue2.add(task);
        }else{
            queue3.add(task);
        }
    }

    //剩余资源
    public int[] returnRemainResource(){
        int remainResource[]=new int[3];  //初始化资源总数
        int Resource[]={CPU, RAM, storage};
        /*剩余资源=当前总资源-正在执行任务所占资源*/
        int totalResource[]=new int[3];// 当前所占总资源
        for( EdgeVM vm : activeVM){
            totalResource[0] += vm.getCPU();
            totalResource[1] += vm.getRAM();
            totalResource[2] += vm.getStorage();
        }

        remainResource[0]=Resource[0]-totalResource[0];
        remainResource[1]=Resource[1]-totalResource[1];
        remainResource[2]=Resource[2]-totalResource[2];

        return remainResource;
    }


    //关闭所有任务已经处理完的VM
    public void terminateVMS(double time){
        Iterator<EdgeVM> iteratorVM = activeVM.iterator();
        while(iteratorVM.hasNext()){
            EdgeVM VM = iteratorVM.next();
            if( VM.getOffTime() <= time ) { //等于还是小于等于取决于循环细粒度
                totalNum++;
                //把处理完的任务加入数据统计集合
                Data.addFinishedTasks(VM.getProcessingTask());
                //离队
                iteratorVM.remove();
            }
        }

    }


    /**
     * FCFS资源调度总函数
     * */
    public void processTask_FCFS(double time){

        for( int i=0; i<3; i++ ){ //固定开三台虚拟机
            EdgeVM vm = activeVM.get(i);
            if( vm.getProcessingTask() == null ){ //虚拟机没有运行任务
                if( queue.get(i).size() != 0 ){
                    Task task = queue.get(i).get(0);
                    queue.get(i).remove(0);
                    task.setFinishTime( time + task.length );
                    vm.setProcessingTask(task);
                }
            }else if( vm.getProcessingTask().finishTime == time ){ //虚拟机有该时刻结束的任务
                totalNum++;
                Data.addFinishedTasks(vm.getProcessingTask());
                vm.clearProcessingTask();
                if( queue.get(i).size() != 0 ){
                    Task task = queue.get(i).get(0);
                    queue.get(i).remove(0);
                    task.setFinishTime( time + task.length );
                    vm.setProcessingTask(task);
                }
            }
        }
    }

    /**
     * SJF调度总函数
     * */
    public void processTask_SJF(double time){

        for( List<Task> que : queue ){
            Collections.sort(que, taskLengthComparator);
        }

        for( int i=0; i<3; i++ ){ //固定开三台虚拟机
            EdgeVM vm = activeVM.get(i);
            if( vm.getProcessingTask() == null ){ //虚拟机没有运行任务
                if( queue.get(i).size() != 0 ){
                    Task task = queue.get(i).get(0);
                    queue.get(i).remove(0);
                    task.setFinishTime( time + task.length );
                    vm.setProcessingTask(task);
                }
            }else if( vm.getProcessingTask().finishTime == time ){ //虚拟机有该时刻结束的任务
                totalNum++;
                Data.addFinishedTasks(vm.getProcessingTask());
                vm.clearProcessingTask();
                if( queue.get(i).size() != 0 ){
                    Task task = queue.get(i).get(0);
                    queue.get(i).remove(0);
                    task.setFinishTime( time + task.length );
                    vm.setProcessingTask(task);
                }
            }
        }
    }

    /**
     * MILP调度算法
     * */
    public void processTask_MILP(double time){
        terminateVMS(time);
        List<Task> bufferTaskList = new ArrayList<Task>();
        //所有任务按照长度排序
        for( List<Task> que : queue ){
            bufferTaskList.addAll(que);
        }
        try{
            // 声明cplex优化模型
            IloCplex cplex = new IloCplex();

            // 设定变量及上下限
            int[] lb = new int[bufferTaskList.size()];
            Arrays.fill(lb, 0);
            int[] ub = new int[bufferTaskList.size()];
            Arrays.fill(ub,1);
            IloIntVar[] x = cplex.intVarArray(bufferTaskList.size(),lb,ub);

            //设定目标函数
            IloNumExpr cs1 = cplex.numExpr(); //表达式
            IloNumExpr cs2 = cplex.numExpr();
            for(int i=0; i<bufferTaskList.size();i++){
                Task task = bufferTaskList.get(i);
                double delay = StaticfinalTags.curTime - task.arrivalTime + task.length;
                cs1 = cplex.sum( cs1, cplex.prod(x[i], delay));

                cs2 = cplex.sum( cs2, cplex.prod(x[i], task.CPU));
                cs2 = cplex.sum( cs2, cplex.prod(x[i], task.RAM));
                cs2 = cplex.sum( cs2, cplex.prod(x[i], task.storage));

            }
            cs1 = cplex.prod(cs1, -1);
            cs2 = cplex.prod(cs2, 0.5);
            cs1 = cplex.sum(cs1, cs2);
//            cplex.addMinimize(cs1);
            cplex.addMaximize(cs1);
//            cplex.addMaximize(cs1);



            //设定限制条件
            IloNumExpr cs3 = cplex.numExpr();
            IloNumExpr cs4 = cplex.numExpr();
            IloNumExpr cs5 = cplex.numExpr();
            int[] remainResource = returnRemainResource();
            for(int i=0; i<bufferTaskList.size();i++){
                Task task = bufferTaskList.get(i);
                cs3 = cplex.sum(cs3, cplex.prod(x[i], (double)remainResource[0]));
                cs4 = cplex.sum(cs4, cplex.prod(x[i], (double)remainResource[1]));
                cs5 = cplex.sum(cs5, cplex.prod(x[i], (double)remainResource[2]));
            }
            cplex.addLe(cs3, CPU);
            cplex.addLe(cs4, RAM);
            cplex.addLe(cs5, storage);

            //模型求解
            double[] val = new double[bufferTaskList.size()];
            if (cplex.solve()) {
                // cplex.output()，数据输出，功能类似System.out.println();
//                cplex.output().println("Solution status = " + cplex.getStatus());  // cplex.getStatus：求解状态，成功则为Optimal
                // cplex.getObjValue()：目标函数的最优值
//                cplex.output().println("Solution value = " + cplex.getObjValue());
                // cplex.getValues(x)：变量x的最优值
                val = cplex.getValues(x);
//                for (int j = 0; j < val.length; j++)
//                    cplex.output().println("x" + (j+1) + "  = " + val[j]);
            }
            // 退出优化模型
            cplex.end();


            for (int j = 0; j < bufferTaskList.size(); j++){
//                System.out.println("共" +val.length+ "个，" +"x" + (j+1) + "  = " + val[j]);
                if( val[j] == 1.0 ){
                    Task task = bufferTaskList.get(j);
                    task.setFinishTime( time + task.length);
                    activeVM.add( new EdgeVM(task, time) );
                    queue.get(task.getType()-1).remove(task);
                }
            }

        } catch (IloException e){
            System.err.println("Concert exception caught: " + e);        }
    }

    //关闭所有的Host和VM
    public void shutdownEntity(){

    }

    //比较器构造方法
    public class TaskPreferenceComparator implements Comparator<Task>
    {
        public int compare(Task t1, Task t2)
        {
//            return (t1.taskID - t2.taskID);
            double qmw1 = (queue.get(t1.getType()-1).size()-1) * Math.min(queue.get(t1.getType()-1).size(),1);
            double qmw2 = (queue.get(t2.getType()-1).size()-1) * Math.min(queue.get(t2.getType()-1).size(),1);

//            pen[0] += ( time - queue1.get(m).getArrivalTime() + queue1.get(m).getLength() )/ (3*tmpnum[0]);

            double pen1 = StaticfinalTags.curTime - t1.arrivalTime + t1.length;
            double pen2 = StaticfinalTags.curTime - t2.arrivalTime + t2.length;

            double score1 = qmw1 + StaticfinalTags.alpha*pen1;
            double score2 = qmw2 + StaticfinalTags.alpha*pen2;

            //越小越好
            if( score1 <= score2){
                return -1;
            }else{
                return 1;
            }

        }
    }

    //默认升序
    public class TaskLengthComparator implements Comparator<Task>{
        public int compare(Task t1, Task t2)
        {
            double tmp = t1.length - t2.length;
            if( tmp < 0 ){
                return -1;
            }else if( tmp == 0 ){
                return 0;
            }else{
                return 1;
            }
        }
    }


    //一些没什么用的方法
    public double getX() {   return x_pos;    }
    public void setX(double x_pos) {    this.x_pos = x_pos;    }
    public double getY() {    return y_pos;    }
    public void setY(double y_pos) {    this.y_pos = y_pos;    }
    public int getId(){ return id;}
    public List<Task> getReceiveReqFromTasks() {    return receiveReqFromTasks;   }
    public List<List<Task>> getQueue() {  return queue;  }
    public List<EdgeVM> getActiveVM() { return activeVM; }

    @Override
    public String toString() {
        return "EdgeDataCenter{" +
                "id=" + id +
                ", CPU=" + CPU +
                ", RAM=" + RAM +
                ", storage=" + storage +
                ", x_pos=" + x_pos +
                ", y_pos=" + y_pos + "\r\n" +
                "avtiveVM" + activeVM +
                '}' + "\r\n";
    }
}
