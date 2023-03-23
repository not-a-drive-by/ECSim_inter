package edu.boun.edgecloudsim.edge_server;

import edu.boun.edgecloudsim.core.SimSettings;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

public class EdgeServerManager {

    private List<EdgeDataCenter> edgeServersList = new ArrayList<EdgeDataCenter>();

    public void init() throws Exception {
        startDatacenters();

    }



    //创建单个datacenter
    public EdgeDataCenter createDatacenter(Element datacenterElement){
        //datacenterElement可以提供其他相关的构造参数
        int id = Integer.parseInt(datacenterElement.getElementsByTagName("serverID").item(0).getTextContent());
        Element location = (Element)datacenterElement.getElementsByTagName("location").item(0);
        double x_pos = Double.parseDouble(location.getElementsByTagName("x_pos").item(0).getTextContent());
        double y_pos = Double.parseDouble(location.getElementsByTagName("y_pos").item(0).getTextContent());
        Element host = (Element)datacenterElement.getElementsByTagName("host").item(0);
        int CPU = Integer.parseInt(host.getElementsByTagName("core").item(0).getTextContent());
        int RAM = Integer.parseInt(host.getElementsByTagName("ram").item(0).getTextContent());
        int storage = Integer.parseInt(host.getElementsByTagName("storage").item(0).getTextContent());
        return new EdgeDataCenter( id, CPU, RAM, storage, x_pos, y_pos );
    }

    //创建所有datacenters时调用创建VM、Host、单个datacenter
    //创建xml文件中所有的Datacenters
    public void startDatacenters() throws Exception {
        Document doc = SimSettings.getInstance().getEdgeServersDocument();
        NodeList datacenterList = doc.getElementsByTagName("datacenter");
        for (int i = 0; i < datacenterList.getLength(); i++) {
            Node datacenterNode = datacenterList.item(i);
            Element datacenterElement = (Element) datacenterNode;
            edgeServersList.add(createDatacenter(datacenterElement));
        }
    }

    public void updateServerQuota(){
        for(EdgeDataCenter edgeServer:edgeServersList){
            edgeServer.updateServerQuota();
        }
    }

    public void processTasks_FCFS(double time){
        for(EdgeDataCenter edgeDataCenter : edgeServersList){
            edgeDataCenter.processTask_FCFS(time);
        }
    }

//    public void processTasks_MILP(double time){
//        for(EdgeDataCenter edgeDataCenter : edgeServersList){
//            edgeDataCenter.processTask_MILP(time);
//        }
//    }
//

    public void processTasks_SJF(double time){
        for(EdgeDataCenter edgeDataCenter : edgeServersList){
            edgeDataCenter.processTask_SJF(time);
        }
    }



    //关闭localDatacenters中所有的Datacenter
    public void terminateDatacenters() {
        for(EdgeDataCenter dataCenter: edgeServersList){
            dataCenter.shutdownEntity();
        }
    }

    public List<EdgeDataCenter> getEdgeServersList() {  return edgeServersList;    }
}
