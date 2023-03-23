package edu.boun.edgecloudsim.utils;

import java.util.Random;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.Well19937c;


public class Variable {

    public static ParetoDistribution taskLenRNG = new ParetoDistribution();
    public static PoissonDistribution intervalRNG = new PoissonDistribution(2);

    //信道增益中 信道参数 指数分布
    public static int expRnd (double lambda) {
        Random random = new Random();
        double u = random.nextDouble();

        int x = 0;
        double cdf = 0;
        while (u >= cdf) {
            x ++;
            cdf = 1 - Math.exp(-1.0 * lambda * x);
        }
        return x;
    }

    //任务长度服从帕累托分布
    public static void updateParetoGenerator(double Xmin, double mean){
        double shape = mean / ( mean - Xmin );
        taskLenRNG = new ParetoDistribution( Xmin, shape);
    }
    public static int Pareto_Distribution(){
        double p = taskLenRNG.sample();
        //长度是小数
//        int tmp;
//        tmp =  (int) Math.ceil(p*10);
//        double res = (double) tmp/10;
        //长度是整数
        int res = (int) Math.ceil(p);
        if( res > 25 ) return 15;
        return res;
    }

    //任务到达间隔时间服从泊松分布
    //产生的数值value是符合泊松分布的，均值和方差都是Lamda
    public static void updatePoissonGenerator(double mean){
        intervalRNG = new PoissonDistribution(mean);
    }

    public static int Poisson_Distribution(){
        return intervalRNG.sample();
    }

    /**任务时间间隔服从指数分布*/
    public static int Exp_Distribution (double lambda) {
        Random random = new Random();
        double u = random.nextDouble();
        int x = 0;
        double cdf = 0;
        while (u >= cdf) {
            x ++;
            cdf = 1 - Math.exp(-1.0 * lambda * x);
        }
        return x;
    }


    public static void main(String[] args) {
        updateParetoGenerator(0.1, 1);
        for (int i=0; i<50; i++){
            System.out.println(Pareto_Distribution());
        }
    }


}
