package com.mycompany.proyectomapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CustomTuple implements Writable{
    private int year;
    private double voteMin;
    private double voteMax;
    private double revenueMax;
    private double revenueMin;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public double getVoteMin() {
        return voteMin;
    }

    public void setVoteMin(double voteMin) {
        this.voteMin = voteMin;
    }

    public double getVoteMax() {
        return voteMax;
    }

    public void setVoteMax(double voteMax) {
        this.voteMax = voteMax;
    }

    public double getRevenueMax() {
        return revenueMax;
    }

    public void setRevenueMax(double revenueMax) {
        this.revenueMax = revenueMax;
    }

    public double getRevenueMin() {
        return revenueMin;
    }

    public void setRevenueMin(double revenueMin) {
        this.revenueMin = revenueMin;
    }

    @Override
    public String toString() {
        return "CustomTuple{" + "year=" + year + ", voteMin=" + voteMin + ", voteMax=" + voteMax + ", revenueMax=" + revenueMax + ", revenueMin=" + revenueMin + '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeDouble(voteMin);
        out.writeDouble(voteMax);
        out.writeDouble(revenueMin);
        out.writeDouble(revenueMax);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year=in.readInt();
        voteMin=in.readDouble();
        voteMax=in.readDouble();
        revenueMin=in.readDouble();
        revenueMax=in.readDouble();
    }
    
    
}
