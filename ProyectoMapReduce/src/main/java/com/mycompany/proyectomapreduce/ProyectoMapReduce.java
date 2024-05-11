/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.proyectomapreduce;

/**
 *
 * @author iniet
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.core.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


/**
 *
 * @author alumno
 */
public class ProyectoMapReduce {
    
    
    private static class MediaPresupuesto extends Reducer<Text, Text, Text, IntWritable>{

        private int media = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try{
                media = 0;
                int filas=0;
                for(Text valor : values){
                    String[] str = valor.toString().split("\t", -1);
                    if(!("budget".equals(str[2]))){
                        media +=Integer.parseInt(str[2]);
                        filas+=1;
                    }
                }
                media=media/filas;
                context.write(key, new IntWritable(media));
            }catch(IOException | InterruptedException  e){
                System.err.println("Capturada Excepcion: " + e);
                System.err.println("Mensaje: " + e.getMessage());
                e.printStackTrace();                    
            }
        }
        
        
    }
    
    private static class PartitionerClassPelicula extends Partitioner<Text,Text> {

        @Override
        public int getPartition(Text key, Text value, int i) { //Sacamos la edad del parametro value
            String[] str = value.toString().split("\t",-1);
            String diaSemana =(str[16]);
            if(("Monday".equals(diaSemana) || "Wednesday".equals(diaSemana) || "Tuesday".equals(diaSemana)) && !("day_of_week".equals(diaSemana))){
                return 0;
            }
            else if("Thursday".equals(diaSemana) || "Friday".equals(diaSemana)){
                return 1;
            }else{
                return 2;
            }
        }
        
    }
    
    public static void writeFileToHDFS(String localFileName, String hdfsFilePath) throws IOException, Exception {
 //Setting up the details of the configuration
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.10.1:9000"), configuration, "a_83047");
        //Create a path
        Path hdfsWritePath = new Path(hdfsFilePath + localFileName);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);
        BufferedReader br = new BufferedReader( new FileReader(hdfsFilePath + localFileName));
        BufferedWriter bufferedWriter = new BufferedWriter( new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
        String linea;
        while ((linea = br.readLine())!=null){
            bufferedWriter.write(linea);
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
        br.close();
        fileSystem.close();
    }


   public static void main(String[] args) {
   }
}

