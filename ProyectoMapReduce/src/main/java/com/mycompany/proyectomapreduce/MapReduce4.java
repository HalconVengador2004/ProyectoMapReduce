package com.mycompany.proyectomapreduce;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.security.UserGroupInformation;
import java.util.ArrayList;

public class MapReduce4 {

    public static void writeFileToHDFS(String hadoopRoute, String localRoute) throws IOException {
        //Setting up the details of the configuration
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        try {
            try {
                fileSystem = FileSystem.get(new URI("hdfs://192.168.10.1:9000"), configuration, "a_83045");
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
                fileSystem = FileSystem.get(new URI("hdfs://192.168.10.1:9000"), configuration, "a_83045");
            } catch (InterruptedException ex) {
                System.err.println("Capturada Excepcion " + ex);
                System.err.println("Mensaje: " + ex.getMessage());
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

    //Map class
    public static class MapClass extends Mapper<LongWritable, Text, Text, CustomTuple1> {

        private CustomTuple1 outTuple = new CustomTuple1();

        public void map(LongWritable key, Text value, Reducer.Context context) {
            try {
                String data = value.toString();
                String[] field = data.split("\t", -1);
                if (field != null && field.length == 23) {
                    if (!"original_language".equals(field[3])) {
                        outTuple.setTitulo(field[4]);
                        outTuple.setYear(Integer.parseInt(field[18]));
                        context.write(new Text(field[3]), outTuple);
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

    public static class ReduceClass2 extends Reducer<Text, CustomTuple1, Text, CustomTuple1> {

        private double lTituloTotal = 0;
        private double lTitulo;
        private int count = 0;
        private CustomTuple1 tuple = new CustomTuple1();
        private double media;

        public void reduce(Text key, Iterable<CustomTuple1> values, Reducer.Context context) throws IOException, InterruptedException {
            count = 0;
            ArrayList<Double> lista = new ArrayList();
            for (CustomTuple1 val : values) {
                lTitulo = val.getTitulo().length();
                lTituloTotal = lTituloTotal + lTitulo;
                count = count + 1;
                lista.add(lTitulo);
            }
            media = lTituloTotal / count;
            tuple.setmT(media);
            lTituloTotal = 0;
            for (double val : lista) {
                lTituloTotal = lTituloTotal + Math.pow(val - media, 2);
            }
            lTituloTotal = lTituloTotal / count;
            lTituloTotal = Math.pow(lTituloTotal, 0.5);
            tuple.setdT(lTituloTotal);
            context.write(key, tuple);
        }

    }

    //Partitioner class
    public static class PartitionerClassPelicula extends Partitioner<Text, CustomTuple1> {

        @Override
        public int getPartition(Text key, CustomTuple1 value, int i) {

            double anio = value.getYear();
            if (anio < 1950) {
                return 0;
            } else if (anio >= 1950 && anio < 1965) {
                return 1;
            } else if (anio >= 1965 && anio < 1975) {
                return 2;
            } else if (anio >= 1975 && anio < 1990) {
                return 3;
            } else if (anio >= 1990 && anio < 2000) {
                return 4;
            } else {
                return 5;
            }
        }

    }

    public static void main(String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("a_83045");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", "hdfs://192.168.10.1:9000");
                Job job = Job.getInstance(conf, "MapReduce4");
                job.setJarByClass(Mapreduce4.class);
                job.setMapperClass(MapClass.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(CustomTuple1.class);
                job.setReducerClass(ReduceClass2.class);
                job.setPartitionerClass(PartitionerClassPelicula.class);
                job.setNumReduceTasks(6); // Ensure there are enough reducers for partitions
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(CustomTuple1.class);

                FileInputFormat.addInputPath(job, new Path("/PCD2024/a_83045/movies"));
                FileOutputFormat.setOutputPath(job, new Path("/PCD2024/a_83045/salidaHadoop"));
                FileSystem hdfs = FileOutputFormat.getOutputPath(job).getFileSystem(conf);
                String hadoopRoute = "/PCD2024/a_83045/movies/movies.tsv";
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
                readFileFromHDFS("/PCD2024/a_83045/salidaHadoop/part-r-00000");
                System.out.println("anio >=1950 && anio < 1965");
                readFileFromHDFS("/PCD2024/a_83045/salidaHadoop/part-r-00001");
                System.out.println("anio >= 1965 && anio < 1975");
                readFileFromHDFS("/PCD2024/a_83045/salidaHadoop/part-r-00002");
                System.out.println("anio >= 1975 && anio < 1990");
                readFileFromHDFS("/PCD2024/a_83045/salidaHadoop/part-r-00003");
                System.out.println("anio >= 1990 && anio < 2000");
                readFileFromHDFS("/PCD2024/a_83045/salidaHadoop/part-r-00004");
                System.out.println("anio >= 2000");
                readFileFromHDFS("/PCD2024/a_83045/salidaHadoop/part-r-00005");

                return null;
            }
        });
    }

}
