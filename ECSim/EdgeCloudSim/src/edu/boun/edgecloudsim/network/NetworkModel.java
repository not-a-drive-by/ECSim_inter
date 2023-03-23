package edu.boun.edgecloudsim.network;

import edu.boun.edgecloudsim.core.SimManager;
import edu.boun.edgecloudsim.edge_client.MobileDevice;
import edu.boun.edgecloudsim.edge_server.EdgeDataCenter;

import java.util.ArrayList;
import java.util.List;

public class NetworkModel {
    private List<MobileDevice> devicesList;
    private List<EdgeDataCenter> serversList;
    private List<Channel> channelsList = new ArrayList<Channel>();

    public NetworkModel( ){    }

    public void init(List<MobileDevice> _devicesList, List<EdgeDataCenter> _serversList){
        devicesList = _devicesList;
        serversList = _serversList;

        for( MobileDevice mobileDevice : devicesList ){
            for( EdgeDataCenter edgeServer : serversList ){
                channelsList.add( new Channel( mobileDevice, edgeServer ) );
            }
        }
    }

    public List<Channel> serachChannelByDevice(int mobileID){
        List<Channel> res = new ArrayList<Channel>();
        for( Channel channel : channelsList ){
            if(channel.getMobileDevice().getMobileID() == mobileID ){
                res.add(channel);
            }
        }
        return res;
    }

    public Channel serachChannelByDeviceandServer(int mobileID, int serverID){
        Channel res = null; // 不初始化没法用
        for( Channel channel : channelsList ){
            if(channel.getMobileDevice().getMobileID()==mobileID && channel.getEdgeServer().getId()==serverID){
                res = channel;
            }
        }
        return res;
    }

    //无用函数
    public List<Channel> getChannelsList() {    return channelsList;    }
}
