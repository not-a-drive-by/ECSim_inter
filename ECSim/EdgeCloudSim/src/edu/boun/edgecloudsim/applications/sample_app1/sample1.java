package edu.boun.edgecloudsim.applications.sample_app1;

import edu.boun.edgecloudsim.core.ScenarioFactory;
import edu.boun.edgecloudsim.core.SimManager;
import edu.boun.edgecloudsim.core.SimSettings;
import edu.boun.edgecloudsim.edge_client.MobileDeviceManager;
import edu.boun.edgecloudsim.edge_server.EdgeDataCenter;
import edu.boun.edgecloudsim.statistic.Data;
import edu.boun.edgecloudsim.utils.StaticfinalTags;

public class sample1 {
    public static void main(String[] args) throws Exception {
        //在这里开始仿真

        //读取配置文件
        SimSettings SS = SimSettings.getInstance();
        SS.init("ECSim/EdgeCloudSim/scripts/sample_app1/config/mobile_devices.xml",
                "ECSim/EdgeCloudSim/scripts/sample_app1/config/edge_servers.xml");

        String orchestratorPolicy = "Matching";
//        String orchestratorPolicy = "Random";
//        String orchestratorPolicy = "MILP";
//        String orchestratorPolicy = "POCSA";

        //创建实体类集合类
        ScenarioFactory scenarioFactory = new SampleScenarioFactory(orchestratorPolicy);
        //创建实体类集合类的管理类
        SimManager simManager = new SimManager(scenarioFactory, 3, "Matching",orchestratorPolicy);


        //时隙大循环
        for(int t=0; t<1024; t++){
            StaticfinalTags.curTime++;
            //1. 更新mobileDevice的待处理待发送队列 edgeServer的待处理队列
            simManager.updateQueues(t, scenarioFactory);
            //2. 更新mobileDevice的坐标
            simManager.getMobilityModel().updateLocation();
            //3. 更新信道状态
            simManager.updateChannel();
            //4. 确定移动设备参与匹配的名额
            simManager.generateQuota();
            //5. Edge Orchestrator负责完成设备与服务器之间的匹配
            simManager.getEdgeOrchestrator().Matching(simManager.getNetworkModel());
            //6. 根据匹配结果开始传输数据包
            simManager.transmitteTasks();
            //7. edgeServer开始进行资源调度
            simManager.processTask(t);
        }

        //输出统计结果
        System.out.println( "完成了" + Data.getFinishedTaskSum() + "个任务，平均时延" + Data.overallDelay()  );

        for(EdgeDataCenter server: simManager.getEdgeServerManager().getEdgeServersList()){
            System.out.println(server.getId()+" "+server.totalNum);
        }
        //结束
        simManager.shutdownEntity();


    }
}
