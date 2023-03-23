/*
* 全局一个MobileDeviceManager
* MobileDevice注册中心 得到所有的mobile device实体
* 负责绑定Task、提交任务
* */

package edu.boun.edgecloudsim.edge_client;

import edu.boun.edgecloudsim.core.ScenarioFactory;
import edu.boun.edgecloudsim.core.SimSettings;
import edu.boun.edgecloudsim.edge_orchestrator.DefaultEdgeOrchestrator;
import edu.boun.edgecloudsim.edge_orchestrator.EdgeOrchestrator;
import edu.boun.edgecloudsim.edge_server.EdgeServerManager;
import edu.boun.edgecloudsim.network.NetworkModel;
import edu.boun.edgecloudsim.task_generator.DeviceTaskStatic;
import edu.boun.edgecloudsim.task_generator.Task;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class MobileDeviceManager {

    private List<MobileDevice> mobileDevicesList = new ArrayList<MobileDevice>();
    public SimSettings SS = SimSettings.getInstance();
    private ArrayList<List<Task>> taskList; //从文件读取出来的任务的存放变量

    //初始化移动设备
    public void initMobileDevice() throws Exception {
        int MobileDeviceNum = SS.MobileDeviceNum;
        //读出任务
        List<Task> t = null;
        FileInputStream fi=new FileInputStream("TaskInformation.txt");
        ObjectInputStream si=new ObjectInputStream(fi);
        taskList = new ArrayList<List<Task>>();
        try
        {
            taskList.clear();
            for(int i=0;i<MobileDeviceNum;i++)
            {
                t=(List<Task>)si.readObject();
                taskList.add(t);
            }
            si.close();
        }
        catch(IOException e)
        {
            System.out.println(e.getMessage());
        }
        //创建移动设备 并添加任务
        ArrayList<DeviceTaskStatic> mobileDeviceStatics = SS.getMobileDeviceStatic();
        for (int i = 0; i < mobileDeviceStatics.size(); i++) {
            DeviceTaskStatic mobileDeviceStatic = mobileDeviceStatics.get(i);
            List<Task> tasks = taskList.get(i);
            mobileDevicesList.add(createMobileDevice(mobileDeviceStatic,tasks));
        }

    }

    private MobileDevice createMobileDevice(DeviceTaskStatic mobileDeviceStatic, List<Task> tasks){
        return new MobileDevice(mobileDeviceStatic.x_pos, mobileDeviceStatic.y_pos,
                mobileDeviceStatic.deviceID, tasks);
    }

    //网络模型初始化后 给用户的所有任务绑定网络模型
    public void bindTaskNetworkModel(NetworkModel networkModel){
        for(MobileDevice mobileDevice : mobileDevicesList ){
            mobileDevice.bindNetworkModel(networkModel);
        }
    }

    //更新待处理任务
    public void updateUnprocessedQueues(int t){
        for( MobileDevice mobileDevice : mobileDevicesList ){
            mobileDevice.updateDeviceQueue(t);
        }
    }



    //Matching模式下 更新队列quota 并向Edge Orchestrator提交任务
    public void updateQuotas(NetworkModel networkModel, EdgeOrchestrator edgeOrchestrator){
        for( MobileDevice mobileDevice : mobileDevicesList ){
            mobileDevice.updateQuota(networkModel, edgeOrchestrator);
        }
    }

    //Random模式下 把所有任务都作为待匹配任务 向Edge Orchestrator提交任务
    public void updateRandom(EdgeServerManager edgeServerManager, EdgeOrchestrator edgeOrchestrator){
        for( MobileDevice mobileDevice : mobileDevicesList ){
            mobileDevice.addAllTasks(edgeServerManager, edgeOrchestrator);
        }
    }


    //更新待卸载任务
    public void updateTransQueue_Match(NetworkModel networkModel){
        for( MobileDevice mobileDevice : mobileDevicesList ){
            mobileDevice.updateTransQueue_Match(networkModel);
        }
    }

    public void updateTransQueue_Random(NetworkModel networkModel){
        for( MobileDevice mobileDevice : mobileDevicesList ){
            mobileDevice.updateTransQueue_Random(networkModel);
        }
    }
    public void terminateDatacenters() {
        //local computation is not supported in default Mobile Device Manager
    }


    //无用函数
    public List<MobileDevice> getMobileDevicesList() {       return mobileDevicesList;    }
}
