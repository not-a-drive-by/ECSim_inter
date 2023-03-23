/*
*
* */

package edu.boun.edgecloudsim.applications.sample_app1;

import edu.boun.edgecloudsim.core.ScenarioFactory;
import edu.boun.edgecloudsim.edge_client.MobileDeviceManager;
import edu.boun.edgecloudsim.edge_orchestrator.*;
import edu.boun.edgecloudsim.edge_server.EdgeServerManager;
import edu.boun.edgecloudsim.mobility.MobilityModel;
import edu.boun.edgecloudsim.network.NetworkModel;

public class SampleScenarioFactory implements ScenarioFactory {

    private int numOfMobileDevice;
    private double simulationTime;
    private String orchestratorPolicy;
    private String simScenario;

    SampleScenarioFactory(String _orchestratorPolicy){
        orchestratorPolicy = _orchestratorPolicy;
    }

    SampleScenarioFactory(int _numOfMobileDevice,
                          double _simulationTime,
                          String _orchestratorPolicy,
                          String _simScenario){
        orchestratorPolicy = _orchestratorPolicy;
        numOfMobileDevice = _numOfMobileDevice;
        simulationTime = _simulationTime;
        simScenario = _simScenario;
    }





//    @Override
//    public MobilityModel getMobilityModel() {
//        return new NomadicMobility(numOfMobileDevice,simulationTime);
//    }
//
    @Override
    public NetworkModel getNetworkModel() {
        return new NetworkModel();
    }

    @Override
    public EdgeServerManager getEdgeServerManager() {
        return new EdgeServerManager();
    }

    @Override
    //要能够返回不同的编排器
//    public <T>T getEdgeOrchestrator(EdgeServerManager edgeServerManager) {
//
//        if( orchestratorPolicy.equals("Matching") ){
//            return (T) new DefaultEdgeOrchestrator(edgeServerManager);
//        }else if( orchestratorPolicy.equals( "Random" ) ){
//            return (T) new RandomEdgeOrchestrator(edgeServerManager);
//        }
//        return (T)
//    }
    public EdgeOrchestrator getEdgeOrchestrator(EdgeServerManager edgeServerManager) {

        if( orchestratorPolicy.equals("Matching") ){
            return (EdgeOrchestrator) new DefaultEdgeOrchestrator(edgeServerManager);
        }else if( orchestratorPolicy.equals( "Random" ) ){
            return (EdgeOrchestrator) new RandomEdgeOrchestrator(edgeServerManager);
        }else if( orchestratorPolicy.equals( "MILP" ) ){
            return (EdgeOrchestrator) new MILPEdgeOrchestrator(edgeServerManager);
        }else if( orchestratorPolicy.equals( "POCSA" )){
            return (EdgeOrchestrator) new POCSAEdgeOrchestrator(edgeServerManager);
        }
        return (EdgeOrchestrator) new DefaultEdgeOrchestrator(edgeServerManager);
    }

    @Override
    public MobileDeviceManager getMobileDeviceManager() throws Exception {
        return new MobileDeviceManager();
    }

    @Override
    public MobilityModel getMobilityModel( MobileDeviceManager mobileDeviceManager){
        return new MobilityModel(mobileDeviceManager);
    }


}
