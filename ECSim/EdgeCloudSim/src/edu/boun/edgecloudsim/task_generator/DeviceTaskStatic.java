package edu.boun.edgecloudsim.task_generator;

public class DeviceTaskStatic {
    public int taskNum;
    public int deviceID;

    public double type1Ratio;
    public double type2Ratio;
    public double type3Ratio;

    public int meanLen1;
    public int meanLen2;
    public int meanLen3;

    public double x_pos;
    public double y_pos;

    public DeviceTaskStatic(int taskNum, double type1Ratio, double type2Ratio, double type3Ratio,
                            int meanLen1, int meanLen2, int meanLen3,
                            double x, double y, int deviceID) {
        this.taskNum = taskNum;
        this.deviceID = deviceID;
        this.type1Ratio = type1Ratio;
        this.type2Ratio = type2Ratio;
        this.type3Ratio = type3Ratio;
        this.meanLen1 = meanLen1;
        this.meanLen2 = meanLen2;
        this.meanLen3 = meanLen3;
        this.x_pos = x;
        this.y_pos = y;
    }

    @Override
    public String toString() {
        return "DeviceTaskStatic{" +
                "taskNum=" + taskNum +
                ", deviceID=" + deviceID +
                ", type1Ratio=" + type1Ratio +
                ", type2Ratio=" + type2Ratio +
                ", type3Ratio=" + type3Ratio +
                ", meanLen1=" + meanLen1 +
                ", meanLen2=" + meanLen2 +
                ", meanLen3=" + meanLen3 +
                ", x_pos=" + x_pos +
                ", y_pos=" + y_pos +
                '}';
    }
}
