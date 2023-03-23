package edu.boun.edgecloudsim.applications.sample_app1;

import ilog.concert.*;
import ilog.cplex.IloCplex;

public class test {
    public static void main(String[] args) {
        try {
            // 声明cplex优化模型
            IloCplex cplex = new IloCplex();

            // 设定变量上下限
            double[] lb = {0.0, 0.0, 0.0};  // 下限
            double[] ub = {40.0, Double.MAX_VALUE, Double.MAX_VALUE};  // 上限
            IloNumVar[] x = cplex.numVarArray(3, lb, ub);  // 定义优化变量：IloNumVar，3维，以及对应的边界

            // 设定目标函数
            double[] objvals = {1.0, 2.0, 3.0};  // 目标函数系数
            cplex.addMaximize(cplex.scalProd(x, objvals));  // 定义目标函数：addMaximize最大化，scalProd,连乘

            // 设定约束条件
            double[] coeff1 = {-1.0, 1.0, 1.0};  // 第一组约束条件的系数
            double[] coeff2 = {1.0, -3.0, 1.0};  // 第二组约束条件的系数
            cplex.addLe(cplex.scalProd(x, coeff1), 20.0);  // 定义第一组约束条件的系数，addLe(a,b)：a小于等于b
            cplex.addLe(cplex.scalProd(x, coeff2), 30.0);  // 定义第二组约束条件的系数

            // cplex.solve()：模型求解
            double[] val = new double[3];
            if (cplex.solve()) {
                // cplex.output()，数据输出，功能类似System.out.println();
//                cplex.output().println("Solution status = " + cplex.getStatus());  // cplex.getStatus：求解状态，成功则为Optimal
                // cplex.getObjValue()：目标函数的最优值
//                cplex.output().println("Solution value = " + cplex.getObjValue());
                // cplex.getValues(x)：变量x的最优值
                val = cplex.getValues(x);
//                for (int j = 0; j < val.length; j++)
//                    cplex.output().println("x" + (j+1) + "  = " + val[j]);
            }
            // 退出优化模型
            cplex.end();


//            for (int j = 0; j < val.length; j++){
//                System.out.println("x" + (j+1) + "  = " + val[j]);
//            }


        } catch(IloException e) {
            System.err.println("Concert exception caught: " + e);
        }
    }
}
