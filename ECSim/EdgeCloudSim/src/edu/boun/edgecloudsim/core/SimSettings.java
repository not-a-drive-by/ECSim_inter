/*
* 所有从配置文件读出来的数据都从SimSettings实体的属性里获得
* 这些数据再作为参数传入需要的各个实体类里
* */

package edu.boun.edgecloudsim.core;

import edu.boun.edgecloudsim.task_generator.DeviceTaskStatic;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;

public class SimSettings {

    private static SimSettings instance = null;

    //移动用户参数
    private Document mobileDevicesDoc = null;
    private String mobileUsersFile = "ECSim/EdgeCloudSim/scripts/sample_app1/config/mobile_devices.xml";
    public int MobileDeviceNum;
    public ArrayList<DeviceTaskStatic> mobileDeviceStatic = new ArrayList<>();
    public Document getMobileDevicesDocument(){
        return mobileDevicesDoc;
    }


    //边缘计算节点参数
    private Document edgeServersDoc = null;
    private String edgeServersFile = "ECSim/EdgeCloudSim/scripts/sample_app1/config/edge_servers.xml";
    public int EdgeServerNum;
    public ArrayList<DeviceTaskStatic> EdgeDServersStatic = new ArrayList<>();
    public Document getEdgeServersDocument(){
        return edgeServersDoc;
    }


    public static SimSettings getInstance(){
        if(instance == null) {
            instance = new SimSettings();
        }
        return instance;
    }
    public ArrayList<DeviceTaskStatic> getMobileDeviceStatic(){
        return mobileDeviceStatic;
    }

    //初始化任务时只parse一个文件
    public void init(String mobileUsersFile){
        parseMobileDevicesXML(mobileUsersFile);

    }

    public void init(String mobileUsersFile, String edgeServerFile){
        parseMobileDevicesXML(mobileUsersFile);
        parseEdgeServersXML(edgeServerFile);
    }





    public void parseMobileDevicesXML(String filePath) {
        try {
            File devicesFile = new File(filePath);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            mobileDevicesDoc = dBuilder.parse(devicesFile);
            mobileDevicesDoc.getDocumentElement().normalize();

            NodeList userList = mobileDevicesDoc.getElementsByTagName("user");
            MobileDeviceNum = userList.getLength();
            for(int i = 0; i < MobileDeviceNum; i++){
                Node user = userList.item(i);
                Element mobileDeviceElement = (Element) user;

                int deviceID = Integer.parseInt(mobileDeviceElement.getElementsByTagName("userID").item(0).getTextContent());
                int taskNum = Integer.parseInt(mobileDeviceElement.getElementsByTagName("taskNum").item(0).getTextContent());
                double type1Ratio = Double.parseDouble(mobileDeviceElement.getElementsByTagName("type1Ratio").item(0).getTextContent());
                double type2Ratio = Double.parseDouble(mobileDeviceElement.getElementsByTagName("type2Ratio").item(0).getTextContent());
                double type3Ratio = Double.parseDouble(mobileDeviceElement.getElementsByTagName("type3Ratio").item(0).getTextContent());
                int Len1 = Integer.parseInt(mobileDeviceElement.getElementsByTagName("Len1").item(0).getTextContent());
                int Len2 = Integer.parseInt(mobileDeviceElement.getElementsByTagName("Len2").item(0).getTextContent());
                int Len3 = Integer.parseInt(mobileDeviceElement.getElementsByTagName("Len3").item(0).getTextContent());
                Element location = (Element)mobileDeviceElement.getElementsByTagName("location").item(0);
                double x = Double.parseDouble(location.getElementsByTagName("x_pos").item(0).getTextContent());
                double y = Double.parseDouble(location.getElementsByTagName("y_pos").item(0).getTextContent());


                DeviceTaskStatic userStatic = new DeviceTaskStatic(taskNum,type1Ratio,type2Ratio,type3Ratio,Len1,Len2,Len3,x,y,deviceID);
                mobileDeviceStatic.add(userStatic);

            }
        } catch (Exception e) {
            System.out.println("Mobile Devices XML cannot be parsed! Terminating simulation...");
            e.printStackTrace();
            System.exit(1);
        }

    }

    public void parseEdgeServersXML(String filePath){
        try{
            File devicesFile = new File(filePath);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            edgeServersDoc = dBuilder.parse(devicesFile);
            edgeServersDoc.getDocumentElement().normalize();
        }catch(Exception e){
            System.out.println("Edge Devices XML cannot be parsed! Terminating simulation...");
            e.printStackTrace();
            System.exit(1);
        }
    }

}
