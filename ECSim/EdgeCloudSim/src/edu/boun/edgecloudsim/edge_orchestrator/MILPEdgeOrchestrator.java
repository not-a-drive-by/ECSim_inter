package edu.boun.edgecloudsim.edge_orchestrator;

import edu.boun.edgecloudsim.edge_server.EdgeDataCenter;
import edu.boun.edgecloudsim.edge_server.EdgeServerManager;
import edu.boun.edgecloudsim.network.Channel;
import edu.boun.edgecloudsim.network.NetworkModel;
import edu.boun.edgecloudsim.task_generator.Task;

import java.util.Arrays;
import java.util.List;
import ilog.concert.*;
import ilog.cplex.IloCplex;

public class MILPEdgeOrchestrator extends EdgeOrchestrator{

    public MILPEdgeOrchestrator(EdgeServerManager edgeServerManager){
        this.EdgeServers = edgeServerManager.getEdgeServersList();
    }

    public static void main(String[] args) {
        int[] arr = new int[3];
        Arrays.fill(arr,1);
        arr[1] = 2;
        System.out.println( Arrays.toString(arr));
    }

    //最小传输时延 即找最大传输速率的信道
    @Override
    public void Matching(NetworkModel networkModel){
        int m = preMatchTasks.size();
        int c = EdgeServers.size();
        if( m != 0 && c!= 0){
            try{
                // 声明cplex优化模型
                IloCplex cplex = new IloCplex();

                // 设定变量及上下限
                IloIntVar[][] x = new IloIntVar[m][];
                for (int i=0; i<m; i++)
                {
                    x[i] = cplex.intVarArray(c, 0, 1);
                }

                //设定目标函数
                IloNumExpr cs1 = cplex.numExpr(); //最大化资源表达式
                IloNumExpr cs2 = cplex.numExpr(); //最小化时延表达式

                for(int i=0; i<m; i++){ //task
                    for(int j=0; j<c; j++){ //server
                        EdgeDataCenter vm = EdgeServers.get(j);
                        Task task = preMatchTasks.get(i);
                        int resourceSum = vm.CPU + vm.RAM + vm.storage;
                        Channel cha = networkModel.serachChannelByDeviceandServer(
                                task.getMobileDevice().getMobileID(), vm.getId());
                        cs1 = cplex.sum( cs1, cplex.prod(x[i][j], resourceSum));
                        cs2 = cplex.sum( cs2, cplex.prod(x[i][j], preMatchTasks.get(i).getDataSize()/cha.ratio ));
                    }
                }
                cs2 = cplex.prod(cs2, -1);
                cs1 = cplex.sum( cs1, cs2);
                cplex.addMaximize(cs1);


                //设定限制条件
                IloNumExpr cs3 = cplex.numExpr();
                for(int i=0; i<m; i++){ //task
                    for(int j=0; j<c; j++){ //server

                        cs3 = cplex.sum( cs3, x[i][j]);
                    }
                    cplex.addEq(cs3, 1);
                    cs3 = cplex.numExpr(); //每一行新建一个
                }

                //模型求解
                double[][] val = new double[m][c];
                if (cplex.solve()) {
                    for (int i=0; i<m; i++){
                        val[i] = cplex.getValues(x[i]);
                    }
                }
                // 退出优化模型
                cplex.end();

                //根据结果处理
                for(int i=0; i<m; i++){ //task
                    for(int j=0; j<c; j++){ //server
//                        System.out.print("x" +(i+1)+ (j+1) + "  = " + val[i][j]+"  ");
                        if( val[i][j] == 1.0){
                            preMatchTasks.get(i).setTargetServer(EdgeServers.get(j));
                        }
                    }
//                    System.out.println(" ");
                }


            }catch(IloException e){
                System.err.println("Concert exception caught: " + e);
            }
        }

    }


    //无聊函数
    public List<Task> getPreMatchTasks() {   return preMatchTasks;   }
    public void setPreMatchTasks(List<Task> preMatchTasks) {    this.preMatchTasks = preMatchTasks;   }
}
