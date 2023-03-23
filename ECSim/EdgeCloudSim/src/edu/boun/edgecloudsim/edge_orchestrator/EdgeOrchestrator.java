/*
* 集中管理所有的EdgeServer+CloudServer
* 节点间任务卸载至对应的服务器上
* （创建虚拟机交给DataCenter）
* */

package edu.boun.edgecloudsim.edge_orchestrator;

import edu.boun.edgecloudsim.edge_server.EdgeDataCenter;
import edu.boun.edgecloudsim.network.NetworkModel;
import edu.boun.edgecloudsim.task_generator.Task;

import java.util.ArrayList;
import java.util.List;

public abstract class EdgeOrchestrator {
    public List<Task> preMatchTasks = new ArrayList<Task>();
    public List<EdgeDataCenter> EdgeServers;

    public void clearPrematchTasks(){
        preMatchTasks.clear();
    }

    public void Matching(NetworkModel networkModel){

    }

    public List<Task> getPreMatchTasks() {   return preMatchTasks;   }
    public void setPreMatchTasks(List<Task> preMatchTasks) {    this.preMatchTasks = preMatchTasks;   }

}
