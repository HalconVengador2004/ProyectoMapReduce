package com.mycompany.proyectomapreduce;

import static com.mycompany.proyectomapreduce.MapReduce3.readFileFromHDFS;
import static com.mycompany.proyectomapreduce.MapReduce3.writeFileToHDFS;
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

public class Mapreduce31 {
    //Map class

    public static class MapClass extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) {
            try {
                String data = value.toString();
                String[] field = data.split("\t", -1);
                if (field != null && !"original_language".equals(field[3]) && field.length == 23) {
                    context.write(new Text(field[3]), new Text(value));
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

        @Override
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

            String[] str = value.toString().split("\t", -1);

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

        }

    }

    public static void main(String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("a_83048");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", "hdfs://192.168.10.1:9000");
                Job job = Job.getInstance(conf, "MapReduce3");
                job.setJarByClass(Mapreduce31.class);
                job.setMapperClass(MapClass.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(ReduceClass.class);
                job.setReducerClass(ReduceClass.class);
                job.setPartitionerClass(PartitionerClassPelicula.class);
                job.setNumReduceTasks(4); // Ensure there are enough reducers for partitions
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);

                FileInputFormat.addInputPath(job, new Path("/PCD2024/a_83048/movies"));
                FileOutputFormat.setOutputPath(job, new Path("/PCD2024/a_83048/salidaHadoop"));
                FileSystem hdfs = FileOutputFormat.getOutputPath(job).getFileSystem(conf);
                String hadoopRoute = "/PCD2024/a_83048/movies/movies.tsv";
                String localRoute = "./resources/convert.tsv";
                if (!hdfs.exists(new Path(hadoopRoute))) { //Comprobamos si ya ha sido subido antes de volver a hacerlo para que no de errores
                    System.out.println("Se esta escribiendo en Hadoop...");
                    writeFileToHDFS(hadoopRoute, localRoute);
                    readFileFromHDFS(hadoopRoute); //Comprobamos que se ha subido leyendolo
                }
                if (hdfs.exists(FileOutputFormat.getOutputPath(job))) { //Si existe el directorio de salida lo boramos para que no de errores
                    System.out.println("Borrando directorio...");
                    hdfs.delete(FileOutputFormat.getOutputPath(job), true);
                }

                boolean finalizado = job.waitForCompletion(true);
                System.out.println("Finalizado: " + finalizado);
                System.out.println("anio < 1990");
                readFileFromHDFS("/PCD2024/a_83048/salidaHadoop/part-r-00000");
                System.out.println("anio >= 1990 && anio < 1995");
                readFileFromHDFS("/PCD2024/a_83048/salidaHadoop/part-r-00001");
                System.out.println("anio >= 1995 && anio < 2000");
                readFileFromHDFS("/PCD2024/a_83048/salidaHadoop/part-r-00002");
                System.out.println("anio >= 2000");
                readFileFromHDFS("/PCD2024/a_83048/salidaHadoop/part-r-00003");
                return null;
            }
        });
    }

}
