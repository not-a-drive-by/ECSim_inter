package edu.boun.edgecloudsim.edge_client;

import edu.boun.edgecloudsim.core.ScenarioFactory;
import edu.boun.edgecloudsim.edge_orchestrator.DefaultEdgeOrchestrator;
import edu.boun.edgecloudsim.edge_orchestrator.EdgeOrchestrator;
import edu.boun.edgecloudsim.edge_server.EdgeDataCenter;
import edu.boun.edgecloudsim.edge_server.EdgeServerManager;
import edu.boun.edgecloudsim.network.Channel;
import edu.boun.edgecloudsim.network.NetworkModel;
import edu.boun.edgecloudsim.task_generator.TaskGeneratorModel;
import edu.boun.edgecloudsim.task_generator.Task;

import java.lang.reflect.Array;
import java.util.*;

public class MobileDevice {
    //每个设备有一个产生任务的对象
//    private TaskGeneratorModel taskGenerator;

    //设备自身状态信息
    private int mobileID;
    private double x_pos;
    private double y_pos;
    private int power;//发射功率
    private int threshold = 10;

    //待处理任务队列 假设都是三种
    public Queue queue1 = new Queue();
    public Queue queue2 = new Queue();
    public Queue queue3 = new Queue();
    public List<Queue> queue = new ArrayList<Queue>();

    //待发送任务队列 不分种类
    public List<Task> transQueue = new ArrayList<Task>();

    //所有任务先放在预处理队列中
    public List<Task> preQueue;

    //工具类比较器
    Comparator<Queue> queueComparatorByLen = new QueueComparatorByLen();
    Comparator<Queue> queueComparatorByLQValue = new QueueComparatorByQValue();
    Comparator<Task> taskComparatorByLen = new TaskComparatorBySize();


    public MobileDevice(double _x, double _y, int _mobileID, List<Task> tasks) {
        //初始化参数
        this.x_pos = _x;
        this.y_pos = _y;
        this.mobileID = _mobileID;
        this.preQueue = tasks;
        this.power = 20;

        for( Task task : tasks ){
            task.setDevice(this);
        }
        queue.add(queue1);
        queue.add(queue2);
        queue.add(queue3);

    }

    //绑定网络模型
    public void bindNetworkModel(NetworkModel networkModel){
        for(Task task : preQueue){
            task.setNetworkModel(networkModel);
        }
    }

    //更新待处理队列
    public void updateDeviceQueue(int t){
        if(preQueue.size()==0) return;

        Task tmpTask = preQueue.get(0);
        while(tmpTask.arrivalTime <= t ){
            if( tmpTask.getType()==1 ){
                queue1.getTaskQueues().add(tmpTask);
            }else if( tmpTask.getType()==2 ){
                queue2.getTaskQueues().add(tmpTask);
            }else{
                queue3.getTaskQueues().add(tmpTask);
            }
            preQueue.remove(tmpTask);
            if(preQueue.size()==0) break;
            tmpTask = preQueue.get(0);
        }
    }



    //Matching模式下更新quota
    public void updateQuota(NetworkModel networkModel, EdgeOrchestrator edgeOrchestrator){
        //1.先清除所有队列的quota
        queue.clear();
        queue.add(queue1);
        queue.add(queue2);
        queue.add(queue3);
        for(Queue que : queue){
            que.setQuota(0);
        }
        clearBlankQueue();
        if(queue.size()==0){
            return;
        }
        //2.找出对于当前设备而言的合格信道
        List<Channel> channels = networkModel.serachChannelByDevice(mobileID);
        Iterator<Channel> iteratorChannel = channels.iterator();
        while(iteratorChannel.hasNext()){
            Channel chan = iteratorChannel.next();
            //这一步筛选的只是对于节点而言合格的信道，对任务而言可用的服务器要去orchestrator里筛选
            if( chan.ratio < threshold ) iteratorChannel.remove();
        }
//        System.out.println("更新信道信息后"+channels.size());
        //没有合格信道结束
        if(channels.size() == 0) return;

        List<EdgeDataCenter> acceptableServers = new ArrayList<EdgeDataCenter>();
        for( Channel chan : channels){
            acceptableServers.add( chan.getEdgeServer() );
        }

        //3.否则开始Q值迭代
        int quotaSum = channels.size();
        if( quotaSum < queue.size() ){//3.1：quota不够所有队列分配一个 对队长排序后分配
            Collections.sort(queue, queueComparatorByLen);
            int index = 0;
            while(quotaSum != 0){
                queue.get(index).setQuota(1);
                index++;
                quotaSum--;
            }
        }else if( quotaSum==queue.size() ){//3.2: 每个队列quota一个
            for(Queue que : queue){      que.setQuota(1);     }
        }else{//3.3:有多的 按Q值分配
            quotaSum -= queue.size();
            for(Queue que : queue){      que.setQuota(1);     }
            while( quotaSum!=0 ){
                Collections.sort(queue, queueComparatorByLQValue);
                int tmp = queue.get(0).getQuota();
                queue.get(0).setQuota( tmp+1 );
                quotaSum--;
            }
        }

        //把quota个任务添加到Orcheastator上
        for(Queue que:queue){
            int tmp = que.getQuota();
            int i = 0; //任务不会离队 所以下标记录
            Task tmpTask = null;
            while( tmp != 0 && i<que.getTaskQueues().size()){
                tmpTask = que.getTaskQueues().get(i);
                tmpTask.setPreferenceList(acceptableServers);
                tmpTask.sortPreferenceList(); //产生偏好序列 待补充
                edgeOrchestrator.getPreMatchTasks().add(tmpTask);//提交到Edge Orcheastator上
                tmp--;
                i++;
            }
        }
        return;
    }

    //所有任务都添加到待匹配队列
    //Random模式下
    public void addAllTasks(EdgeServerManager edgeServerManager,EdgeOrchestrator edgeOrchestrator){
        //找出对于当前设备而言的合格信道
        List<EdgeDataCenter> allServers = edgeServerManager.getEdgeServersList();
        for(Queue que : queue){
            List<Task> taskList = que.getTaskQueues();
            for(Task tmpTask : taskList){
                tmpTask.setPreferenceList( allServers );
                edgeOrchestrator.getPreMatchTasks().add(tmpTask);
//                System.out.println("添加到编排器了");
            }
        }
    }

    //queue清除空白队列
    private void clearBlankQueue(){
//        for(Queue que : queue){
//            if(que.getTaskQueues().size()==0) queue.remove(que);
//        }
        Iterator<Queue> iteratorQue = queue.iterator();
        while(iteratorQue.hasNext()){
            Queue que = iteratorQue.next();
            if( que.getTaskQueues().size()==0 ) iteratorQue.remove();
        }
    }

    /**
     * 根据matching结果，将待传输任务加入集合并更新待发送队列
     * */
    public void updateTransQueue_Match(NetworkModel networkModel){
        //根据匹配结果 更新待传输队列
        List<Task> bufferTasks = new ArrayList<Task>();
        for(Queue que:queue){
            int tmp = Math.min(que.getQuota(), que.getTaskQueues().size());
            int i = 0;
            Task tmpTask = null;
            while( tmp != 0 && que.getTaskQueues().size()>0){
                tmpTask = que.getTaskQueues().get(i);
                if(tmpTask.getTargetServer() != null){
                    bufferTasks.add(tmpTask);
                    que.getTaskQueues().remove(tmpTask);
                }else{
                    i++; //没有匹配到 保留在原本的队列里
                }
                tmp--;
            }
        }
        //新到达的待卸载任务排序后依次放入，否则前面传到一半的不传了
        Collections.sort(bufferTasks,taskComparatorByLen);
        transQueue.addAll(bufferTasks);
        //根据信道传输速率 更新每个任务
        Iterator<Task> iteratorTask = transQueue.iterator();
        while( iteratorTask.hasNext() ){
            Task task = iteratorTask.next();
            int serverID = task.getTargetServer().getId();
            Channel cha = networkModel.serachChannelByDeviceandServer( mobileID, serverID);
            if( cha.usedFlag == false ){
                //可以传输不止一个任务、万一同一个设备有多个任务匹配到同一个服务器
                if( cha.ratio > task.getDataSize() ){
                    //能传输完当前任务 信道flag依旧设置为false
                    cha.setRatio( cha.ratio - task.getDataSize() );
                    task.setDataSize( task.getDataSize() - cha.ratio );//每个任务减去该时隙传输的内容大小
                    task.getTargetServer().receiveOffloadTasks(task);
                    iteratorTask.remove();
                }else{
                    //不足以传输完当前任务
                    cha.usedFlag = true;
                    task.setDataSize( task.getDataSize() - cha.ratio );//每个任务减去该时隙传输的内容大小
                }


//                //如果传输完了 就调用对应服务器的接收函数
//                if( task.getDataSize() <= 0 ) {
//                    task.getTargetServer().receiveOffloadTasks(task);
//                    iteratorTask.remove();
//                }
            }
        }

    }

    public void updateTransQueue_Random(NetworkModel networkModel){
        //没有quota限制了
        List<Task> bufferTasks = new ArrayList<Task>();
        //目标服务器不为空的全部待传输
        for(Queue que:queue){
            List<Task> taskList = que.getTaskQueues();
            Iterator<Task> taskIterator = taskList.iterator();
            while(taskIterator.hasNext()){
                Task task = taskIterator.next();
                if(task.getTargetServer() != null){
                    bufferTasks.add(task);
                    taskIterator.remove();
                }
            }
        }

        //新到达的待卸载任务排序后依次放入，否则前面传到一半的不传了
        Collections.sort(bufferTasks,taskComparatorByLen);
        transQueue.addAll(bufferTasks);
        //根据信道传输速率 更新每个任务
        Iterator<Task> iteratorTask = transQueue.iterator();
        while( iteratorTask.hasNext() ){
            Task task = iteratorTask.next();
            int serverID = task.getTargetServer().getId();
            Channel cha = networkModel.serachChannelByDeviceandServer( mobileID, serverID);
            if( cha.usedFlag == false ){
                //可以传输不止一个任务、万一同一个设备有多个任务匹配到同一个服务器
                if( cha.ratio > task.getDataSize() ){
                    //能传输完当前任务 信道flag依旧设置为false
                    cha.setRatio( cha.ratio - task.getDataSize() );
                    task.setDataSize( task.getDataSize() - cha.ratio );//每个任务减去该时隙传输的内容大小
                    task.getTargetServer().receiveOffloadTasks(task);
                    iteratorTask.remove();
                }else{
                    //不足以传输完当前任务
                    cha.usedFlag = true;
                    task.setDataSize( task.getDataSize() - cha.ratio );//每个任务减去该时隙传输的内容大小
                }

            }
        }

    }

    //Queue比较器函数
    public class QueueComparatorByLen implements Comparator<Queue>
    {
        public int compare(Queue q1, Queue q2)
        {
            return (q1.getTaskQueues().size() - q2.getTaskQueues().size());
        }
    }
    public class QueueComparatorByQValue implements Comparator<Queue>
    {
        public int compare(Queue q1, Queue q2)
        {
            double Q1 =  q1.getTaskQueues().size()/(q1.getQuota()*(q1.getQuota()+1));
            double Q2 =  q2.getTaskQueues().size()/(q2.getQuota()*(q2.getQuota()+1));
            if( Q1 > Q2 ){
                return (int) Math.ceil(Q1-Q2);
            }else if(Q1 < Q2){
                return (int) Math.floor(Q1-Q2);
            }else{
                return 0;
            }
        }
    }
    public class TaskComparatorBySize implements Comparator<Task>
    {
        public int compare(Task t1, Task t2)
        {
            double size1 = t1.getDataSize();
            double size2 = t2.getDataSize();
            if( size1 > size2 ){
                return (int) Math.ceil(size1 - size2);
            }else if(size1 < size2){
                return (int) Math.floor(size1 - size2);
            }else{
                return 0;
            }
        }
    }




    //一些无聊的函数
    public double getX() {  return x_pos;    }
    public void setX(double x_pos) {  this.x_pos = x_pos;    }
    public double getY() {    return y_pos;    }
    public void setY(double y_pos) {    this.y_pos = y_pos;    }
    public int getPower(){ return power;}
    public int getMobileID(){   return mobileID; }

    @Override
    public String toString() {
        return "MobileDevice{" +
                "mobileID=" + mobileID +
                ", x_pos=" + x_pos +
                ", y_pos=" + y_pos + "\r\n" +
                ", preProcessedTasks=" + preQueue + "\r\n" +
                ", queue1" + queue1 + "\r\n" +
                ", queue2" + queue2 + "\r\n" +
                ", queue3" + queue3 + "\r\n" +
                ", TRANS" + transQueue + "\r\n" +
                '}' + "\r\n";
    }
}
