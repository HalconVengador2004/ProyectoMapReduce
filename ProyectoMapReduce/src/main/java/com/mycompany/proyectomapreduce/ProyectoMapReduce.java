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
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *
 * @author alumno
 */
public class ProyectoMapReduce {

    public static class MapperClassPelicula extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) {
            try {
                String[] str = value.toString().split(",", -1);
                if (str.length > 15) {
                    String diaSemana = str[15];
                    if (("Monday".equals(diaSemana) || "Wednesday".equals(diaSemana) || "Tuesday".equals(diaSemana)) || "Thursday".equals(diaSemana) || "Friday".equals(diaSemana) && !("day_of_week".equals(diaSemana))) {
                        context.write(new Text(diaSemana), new Text(value));
                    }
                }
            } catch (IOException | InterruptedException ex) {
                System.err.println("Capturada Excepcion  " + ex);
                System.err.println("Mensaje: " + ex.getMessage());
                ex.printStackTrace(System.err);
            }
        }
    }

    private static class ReducerClassPelicula extends Reducer<Text, Text, Text, LongWritable> {
        private long media = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long suma = 0;
            int filas = 0;

            
            for (Text valor : values) {
                String[] str = valor.toString().split(",", -1);

                try {
                    suma += Long.parseLong(str[2]);                   
                    filas += 1;
                } catch (NumberFormatException e) {
                    System.err.println("Capturada NumberFortmatException " + e);
                    System.err.println("Mensaje: " + e.getMessage());
                    e.printStackTrace(System.err);
                    // Log to error file or handle exception
                }

            }
            if (filas > 0) {
                media = suma / filas;
                context.write(key, new LongWritable(media));
            }           
        }

    }

    private static class PartitionerClassPelicula extends Partitioner<Text, Text> { //PorHacer: Tenemos que elegir otro campo por el cual particionar distinto de dia de la semana

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) { 

            String[] str = value.toString().split(",", -1);
            if(numReduceTasks == 0){
                return 0;
            }
            else {
                String anioLanzamiento = str[18];
                int anio = Integer.parseInt(anioLanzamiento);
                if(anio < 1950){
                    return 1;
                }else if(anio >=1950 && anio < 1965){
                    return 2;
                }else if(anio >= 1965 && anio < 1975){
                    return 3;
                }else if(anio >= 1975 && anio < 1990){
                    return 4;
                }else if(anio >= 1990 && anio < 2000){
                    return 5;
                }else{
                    return 6;
                }
                
            } 
            
        }

    }

    public static void writeFileToHDFS(String hadoopRoute, String localRoute) throws IOException {
        //Setting up the details of the configuration
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        try {
            try {
                fileSystem = FileSystem.get(new URI("hdfs://192.168.10.1:9000"), configuration, "a_83048");
            } catch (InterruptedException ex) {
                System.err.println(ex);
                ex.printStackTrace(System.err);
            }
        } catch (URISyntaxException ex) {
            System.err.println(ex);
                ex.printStackTrace(System.err);
        }
        //Create a path
        Path hdfsWritePath = new Path(hadoopRoute);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);
        
        BufferedReader br = new BufferedReader(
                new FileReader(localRoute));
        BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        String linea;
        while ((linea = br.readLine()) != null) {
            bufferedWriter.write(linea);
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
        br.close();
        fileSystem.close();
    }

    public static void readFileFromHDFS(String hadoopRoute) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        try {
            try {
                fileSystem = FileSystem.get(new URI("hdfs://192.168.10.1:9000"), configuration, "a_83048");
            } catch (InterruptedException ex) {
                System.err.println(ex);
                ex.printStackTrace(System.err);
            }
        } catch (URISyntaxException ex) {
            System.err.println("Capturada excepcion" + ex);
            System.err.println("Mensaje: " + ex.getMessage());
            ex.printStackTrace(System.err);
        }

        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(hadoopRoute));
        BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String linea;
        int contador = 1;
        while ((linea = br.readLine()) != null) {
            System.out.println("Linea " + contador + ": " + linea);
            contador++;
        }
        br.close();
        fileSystem.close();
    }

    public static void main(String[] args) {
        
        try {
            writeFileToHDFS( "/PCD2024/a_83048/movies/movies_hadoop.csv","./resources/MOVIES.csv");
            readFileFromHDFS("/PCD2024/a_83048/movies/movies_hadoop.csv");
            UserGroupInformation ugi
                    = UserGroupInformation.createRemoteUser("a_83048");
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://192.168.10.1:9000");
                    Job job = Job.getInstance(conf, "Peliculas");
                    job.setJarByClass(ProyectoMapReduce.class);
                    job.setMapperClass(MapperClassPelicula.class);
                    job.setReducerClass(ReducerClassPelicula.class);
                    job.setPartitionerClass(PartitionerClassPelicula.class);
                    //job.setNumReduceTasks(3);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job,
                            new Path("/PCD2024/a_83048/movies/"));
                    FileOutputFormat.setOutputPath(job,
                            new Path("/PCD2024/a_83048/movies_particionadoHadoop/"));

                    boolean finalizado = job.waitForCompletion(true);
                    System.out.println("Finalizado: " + finalizado);
                    readFileFromHDFS("/PCD2024/a_83048/movies_particionadoHadoop/part-r-00000");
                    //readFileFromHDFS("/PCD2024/a_83048/movies_particionadoHadoop/", "part-r-00001");
                    //readFileFromHDFS("/PCD2024/a_83048/movies_particionadoHadoop/", "part-r-00002");
                    return null;
                }
            });
        } catch (IOException | InterruptedException ex) {
            System.err.println("Capturada Excepcion " + ex);
            System.err.println("Mensaje: " + ex.getMessage());
            ex.printStackTrace(System.err);
        }

    }
}
