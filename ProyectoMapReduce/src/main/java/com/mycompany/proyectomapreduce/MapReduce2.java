package com.mycompany.proyectomapreduce;

import java.io.*;
import java.security.PrivilegedExceptionAction;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import static org.apache.hadoop.security.authentication.server.AuthenticationToken.parse;

public class MapReduce2 {
    //Map class

    public static class MapClass extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Reducer.Context context) {
            try {
                String data = value.toString();
                String[] field = data.split("\t", -1);
                System.out.println(field.length);
                if (field != null && !"original_language".equals(field[3]) && field.length == 23) {
                    context.write(field[3], value);
                }
            } catch (Exception e) {
                System.err.println("Exception: " + e);
                System.out.println("Message: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
    }
//Reducer class
    public static class ReduceClass extends
            Reducer<Text, Text, Text, DoubleWritable> {

        public double max = -1;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            max = -1;
            for (Text val : values) {
                try {
                    String[] str = val.toString().split("\t", -1);
                    double vote = Double.parseDouble(str[12]);
                    if (vote > max) {
                        max = vote;
                    }
                } catch (NumberFormatException e) {
                    Logger.getLogger(ReduceClass.class.getName()).log(Level.SEVERE, "Number format exception: " + e.getMessage(), e);
                }
            }
            context.write(key, new DoubleWritable(max));
        }

    }

    //Partitioner class
    private static class PartitionerClassPelicula extends Partitioner<Text, Text> { //PorHacer: Tenemos que elegir otro campo por el cual particionar distinto de dia de la semana

        @Override
        public int getPartition(Text key, Text value, int i) {

            String[] str = value.toString().split(",", -1);
            if (str.length > 18) {
                String anioLanzamiento = str[18];
                int anio = Integer.parseInt(anioLanzamiento);
                if (anio < 1990) {
                    return 0;
                } else if (anio >= 1990 && anio < 1995) {
                    return 1;
                } else if (anio >= 1995 && anio < 2000) {
                    return 2;
                } else {
                    return 3;
                }
            } else {
                return 4; //La fila tiene menos campos de los esperados
            }

        }

    }

    public static void main(String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("a_83048");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", "hdfs://192.168.10.1:9000");
                Job job = Job.getInstance(conf, "MapReduce3");
                job.setJarByClass(MapReduce2.class);
                job.setMapperClass(MapClass.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(ReduceClass.class);
                job.setReducerClass(ReduceClass.class);
                //job.setPartitionerClass(PartitionerClassPelicula.class);
                //job.setNumReduceTasks(5); // Ensure there are enough reducers for partitions
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                //job.setPartitionerClass(PartitionerClassPelicula.class);

                FileInputFormat.addInputPath(job, new Path("/PCD2024/a_83045/ProyectoArchivo"));
                FileOutputFormat.setOutputPath(job, new Path("/PCD2024/a_83048/4"));

                boolean finalizado = job.waitForCompletion(true);
                System.out.println("Finalizado: " + finalizado);
                return null;
            }
        });
    }

}