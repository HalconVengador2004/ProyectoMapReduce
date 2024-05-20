/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.proyectomapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author pablo
 */
public class CustomTuple1 implements Writable {
    private String titulo;
    private double mT;
    private double dT;
    private int year;

    public double getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getTitulo() {
        return titulo;
    }

    public void setTitulo(String titulo) {
        this.titulo = titulo;
    }

    public double getmT() {
        return mT;
    }

    public void setmT(double mT) {
        this.mT = mT;
    }

    public double getdT() {
        return dT;
    }

    public void setdT(double dT) {
        this.dT = dT;
    }

    @Override
    public String toString() {
        return "CustomTuple1{" + " media longuitud titulo=" + mT + ", desviacion tipica=" + dT + '}';
    }
     @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(mT);
        out.writeDouble(dT);
        out.writeUTF(titulo);
        out.writeInt(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        mT=in.readDouble();
        dT=in.readDouble();
        titulo=in.readUTF();
        year=in.readInt();
    }
}
