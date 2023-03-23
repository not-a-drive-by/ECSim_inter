package edu.boun.edgecloudsim.core;

import edu.boun.edgecloudsim.edge_client.MobileDeviceManager;
import edu.boun.edgecloudsim.edge_orchestrator.DefaultEdgeOrchestrator;
import edu.boun.edgecloudsim.edge_orchestrator.EdgeOrchestrator;
import edu.boun.edgecloudsim.edge_server.EdgeServerManager;
import edu.boun.edgecloudsim.mobility.MobilityModel;
import edu.boun.edgecloudsim.network.NetworkModel;

public interface ScenarioFactory {

    /**
     * provides abstract Edge Orchestrator
     * @param edgeServerManager
     */
    public EdgeOrchestrator getEdgeOrchestrator(EdgeServerManager edgeServerManager);
//    public <T>T getEdgeOrchestrator(EdgeServerManager edgeServerManager);

    /**
     * provides abstract Mobility Model
     */
//    public MobilityModel getMobilityModel();

    /**
     * provides abstract Network Model
     */
    public NetworkModel getNetworkModel();

    /**
     * provides abstract Edge Server Model
     * @return
     */
    public EdgeServerManager getEdgeServerManager();

    /**
     * provides abstract Mobile Device Manager Model
     */
    public MobileDeviceManager getMobileDeviceManager() throws Exception;

    public MobilityModel getMobilityModel(MobileDeviceManager mobileDeviceManager);
}
