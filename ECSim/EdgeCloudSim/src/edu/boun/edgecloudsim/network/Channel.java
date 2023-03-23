package edu.boun.edgecloudsim.network;

import edu.boun.edgecloudsim.edge_client.MobileDevice;
import edu.boun.edgecloudsim.edge_server.EdgeDataCenter;
import edu.boun.edgecloudsim.utils.Variable;

public class Channel {
    private MobileDevice mobileDevice;
    private EdgeDataCenter edgeServer;

    public double ratio;
    public double distance;
    public boolean usedFlag;

    public Channel( MobileDevice mobileDevice, EdgeDataCenter edgeServer){
        this.mobileDevice = mobileDevice;
        this.edgeServer = edgeServer;

        updateRatio();
        this.usedFlag = false;
    }

    private double getDistance(){
        double dx = mobileDevice.getX()-edgeServer.getX();
        double dy = mobileDevice.getY()-edgeServer.getY();
        return Math.sqrt( Math.pow(dx,2) + Math.pow(dy,2) );
    }

    //更新信道传输速率
    public void updateRatio(){
        distance = getDistance();
        double channelGain = Variable.expRnd(1)*Math.pow(distance,-2.5);
        double SINR = channelGain*mobileDevice.getPower()/Math.pow(10,-9);
        this.ratio = Math.log(1+SINR)/Math.log(2);
    }

    //一些没啥用的函数
    public MobileDevice getMobileDevice() {    return mobileDevice;    }
    public EdgeDataCenter getEdgeServer() {      return edgeServer;    }
    public void setRatio(double newRatio) {  this.ratio = newRatio;  }

    @Override
    public String toString() {
        return "Channel{" +
                "mobileDevice=" + mobileDevice.getMobileID() +
                ", edgeServer=" + edgeServer.getId() +
                ", ratio=" + ratio +
                ", usedFlag=" + usedFlag +
                '}' + "\r\n";
    }
}
