package edu.boun.edgecloudsim.edge_orchestrator;

import edu.boun.edgecloudsim.edge_server.EdgeDataCenter;
import edu.boun.edgecloudsim.edge_server.EdgeServerManager;
import edu.boun.edgecloudsim.network.NetworkModel;
import edu.boun.edgecloudsim.task_generator.Task;

import java.util.ArrayList;
import java.util.List;

public class DefaultEdgeOrchestrator extends EdgeOrchestrator{
//    private List<Task> preMatchTasks = new ArrayList<Task>();
//    private List<EdgeDataCenter> EdgeServers;

    public DefaultEdgeOrchestrator( EdgeServerManager edgeServerManager){
        this.EdgeServers = edgeServerManager.getEdgeServersList();
    }

    public void clearPrematchTasks(){
        preMatchTasks.clear();
    }

    @Override
    public void Matching(NetworkModel networkModel){
        boolean fisrtTime = true;
        boolean sendRequestFlag = true;
        boolean rejectRequestFlag = false;
        //匹配前先清空所有服务器收到的请求
        for(EdgeDataCenter edgeDataCenter : EdgeServers){
            edgeDataCenter.getReceiveReqFromTasks().clear();
        }
        while( fisrtTime || !(sendRequestFlag==false || rejectRequestFlag==false) ){
            fisrtTime = false;
            sendRequestFlag = false;
            rejectRequestFlag = false;
            //任务发起请求
//            System.out.println("匹配循环一次");
            for( Task task : preMatchTasks){
//                System.out.println(task.taskID + "偏好序列"+task.getPreferenceList());
                if( task.getPreferenceList().size()!=0 && task.getTargetServer()==null ){
                    sendRequestFlag = true;
                    EdgeDataCenter edgeServer = task.getPreferenceList().get(0);
                    edgeServer.getReceiveReqFromTasks().add(task);
//                    System.out.println(task.taskID + "对"+edgeServer.getId()+"发起了请求");
                    task.setTargetServer(edgeServer);
                    task.getPreferenceList().remove(0);
                }
            }
            //服务器处理请求
            for(EdgeDataCenter edgeServer : EdgeServers){
                rejectRequestFlag = rejectRequestFlag || edgeServer.updatePreference();
            }


        }
        //最后看看匹配结果
        for(Task t : preMatchTasks){
            System.out.println(t.taskID + "匹配到了" +t.getTargetServer());
        }
        for(EdgeDataCenter edgeServer : EdgeServers){
            System.out.println(edgeServer.getId()+"接受了"+edgeServer.getReceiveReqFromTasks());

        }
    }

    //无聊函数
    public List<Task> getPreMatchTasks() {   return preMatchTasks;   }
    public void setPreMatchTasks(List<Task> preMatchTasks) {    this.preMatchTasks = preMatchTasks;   }
}
