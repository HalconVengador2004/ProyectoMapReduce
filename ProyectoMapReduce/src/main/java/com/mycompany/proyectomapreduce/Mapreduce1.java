package com.mycompany.proyectomapreduce;

import static com.mycompany.proyectomapreduce.MapReduce2.readFileFromHDFS;
import static com.mycompany.proyectomapreduce.MapReduce2.writeFileToHDFS;
import java.io.*;
import java.security.PrivilegedExceptionAction;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.security.UserGroupInformation;

public class Mapreduce1 {
    //Map class

    public static class MapClass extends
            Mapper<LongWritable, Text, Text, CustomTuple> {

        private CustomTuple outTuple = new CustomTuple();

        @Override
        public void map(LongWritable key, Text value, Context context) {
            try {
                String data = value.toString();
                String[] field = data.split("\t", -1);
                if (field != null && field.length == 23) {
                    if (!"original_language".equals(field[3])) {
                        outTuple.setRevenueMax(Double.parseDouble(field[8]));

                        outTuple.setRevenueMin(Double.parseDouble(field[8]));

                        outTuple.setVoteMax(Double.parseDouble(field[12]));

                        outTuple.setVoteMin(Double.parseDouble(field[12]));

                        outTuple.setYear(Integer.parseInt(field[18]));

                        context.write(new Text(field[3]), outTuple);
                        System.out.println(outTuple);
                    }
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
            Reducer<Text, CustomTuple, Text, CustomTuple> {

        private double year = 0;
        private int count = 0;
        private CustomTuple tuple = new CustomTuple();

        @Override
        public void reduce(Text key, Iterable<CustomTuple> values, Context context) throws IOException, InterruptedException {

            year = 0;
            count = 0;
            for (CustomTuple val : values) {
                System.out.println(val);
                try {

                    if (tuple.getVoteMax() < val.getVoteMax()) {
                        tuple.setVoteMax(val.getVoteMax());
                    }
                    if (tuple.getVoteMin() > val.getVoteMin()) {
                        tuple.setVoteMin(val.getVoteMin());
                    }
                    if (tuple.getRevenueMax() < val.getRevenueMax()) {
                        tuple.setRevenueMax(val.getRevenueMax());
                    }
                    if (tuple.getRevenueMin() > val.getRevenueMin()) {
                        tuple.setRevenueMin(val.getRevenueMin());
                    }
                    year += val.getYear();
                    count++;

                } catch (NumberFormatException e) {
                    Logger.getLogger(ReduceClass.class.getName()).log(Level.SEVERE, "Number format exception: " + e.getMessage(), e);
                }
            }
            tuple.setYear(year / count);
            context.write(key, tuple);
        }

    }

    //Partitioner class
    private static class PartitionerClassPelicula extends Partitioner<Text, CustomTuple> {

        @Override
        public int getPartition(Text key, CustomTuple value, int i) {

            double anio = value.getYear();
            if (anio < 1950) {
                return 0 % i;
            } else if (anio >= 1950 && anio < 1965) {
                return 1 % i;
            } else if (anio >= 1965 && anio < 1975) {
                return 2 % i;
            } else if (anio >= 1975 && anio < 1990) {
                return 3 % i;
            } else if (anio >= 1990 && anio < 2000) {
                return 4 % i;
            } else {
                return 5 % i;
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
                job.setJarByClass(Mapreduce1.class);
                job.setMapperClass(MapClass.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(CustomTuple.class);
                job.setReducerClass(ReduceClass.class);
                job.setPartitionerClass(PartitionerClassPelicula.class);
                job.setNumReduceTasks(6); // Ensure there are enough reducers for partitions
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(CustomTuple.class);

                FileInputFormat.addInputPath(job, new Path("/PCD2024/a_83048/movies"));
                FileOutputFormat.setOutputPath(job, new Path("/PCD2024/a_83048/salidaHadoop"));
                FileSystem hdfs = FileOutputFormat.getOutputPath(job).getFileSystem(conf);
                String hadoopRoute = "/PCD2024/a_83048/movies/movies.tsv";
                String localRoute = "./resources/output.tsv";
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
                System.out.println("anio < 1950");
                readFileFromHDFS("/PCD2024/a_83048/salidaHadoop/part-r-00000");
                System.out.println("anio >=1950 && anio < 1965");
                readFileFromHDFS("/PCD2024/a_83048/salidaHadoop/part-r-00001");
                System.out.println("anio >= 1965 && anio < 1975");
                readFileFromHDFS("/PCD2024/a_83048/salidaHadoop/part-r-00002");
                System.out.println("anio >= 1975 && anio < 1990");
                readFileFromHDFS("/PCD2024/a_83048/salidaHadoop/part-r-00003");
                System.out.println("anio >= 1990 && anio < 2000");
                readFileFromHDFS("/PCD2024/a_83048/salidaHadoop/part-r-00004");
                System.out.println("anio >= 2000");
                readFileFromHDFS("/PCD2024/a_83048/salidaHadoop/part-r-00005");
                return null;
            }
        });
    }

}
