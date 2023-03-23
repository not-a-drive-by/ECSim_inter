package edu.boun.edgecloudsim.mobility;

import edu.boun.edgecloudsim.edge_client.MobileDevice;
import edu.boun.edgecloudsim.edge_client.MobileDeviceManager;
import edu.boun.edgecloudsim.edge_server.EdgeServerManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MobilityModel {

    private Random r = new Random();
    private List<MobileDevice> mobileDeviceList = new ArrayList<MobileDevice>();

    public MobilityModel( MobileDeviceManager mobileDeviceManager ){
        this.mobileDeviceList = mobileDeviceManager.getMobileDevicesList();
    }

    public void updateLocation(){
        for( MobileDevice mobileDevice : mobileDeviceList){
            generateLocation( mobileDevice );
        }
    }

    public void generateLocation( MobileDevice mobileDevice ){
        int direction = r.nextInt(4); //取值范围是[0,bound-1]
        switch (direction){
            case 0:
                mobileDevice.setY( mobileDevice.getY() + 1 );
                break;
            case 1:
                mobileDevice.setY( mobileDevice.getY() - 1 );
                break;
            case 2:
                mobileDevice.setX( mobileDevice.getX() - 1 );
                break;
            case 3:
                mobileDevice.setX( mobileDevice.getX() + 1 );
                break;
        }
    }

}
