package com.mycompany.proyectomapreduce;

import java.io.*;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.security.UserGroupInformation;

public class MapReduce2 {

    public static class EmployeeMinMaxCountMapper extends Mapper<Object, Text, Text, CustomTuple> {

        private CustomTuple outTuple = new CustomTuple();
        private Text idioma = new Text();

        public void map(Object key, Text value, Reducer.Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] field = data.split("\t", -1);
            System.out.println(field.length);
            if (field != null && !"original_language".equals(field[3]) && field.length==23) {
                System.out.println("1");
                idioma.set(field[3]);
                System.out.println("2");
                System.out.println(field[8]);
                outTuple.setRevenueMax(Double.parseDouble(field[8]));
                System.out.println("3");
                outTuple.setRevenueMin(Double.parseDouble(field[8]));
                System.out.println("4");
                outTuple.setVoteMax(Double.parseDouble(field[12]));
                System.out.println("5");
                outTuple.setVoteMin(Double.parseDouble(field[12]));
                System.out.println("6");
                outTuple.setYear(Integer.parseInt(field[18]));
                System.out.println("7");
                context.write(idioma, outTuple);
            }
        }
    }

    public static class EmployeeMinMaxCountReducer extends Reducer<Text, CustomTuple, Text, CustomTuple> {

        private CustomTuple result = new CustomTuple();

        public void reduce(Text key, Iterable<CustomTuple> values, Reducer.Context context) throws IOException, InterruptedException {

            result.setRevenueMax(0);
            result.setRevenueMin(100000);
            result.setVoteMax(0);
            result.setVoteMin(10);
            result.setYear(0);

            long mYear = 0;
            long count=0;
            for (CustomTuple tuple : values) {
                if (result.getRevenueMax()== 0 || (tuple.getRevenueMax()> result.getRevenueMax())) {
                    result.setRevenueMax(tuple.getRevenueMax());
                }if ((tuple.getRevenueMin()< result.getRevenueMin())) {
                    result.setRevenueMin(tuple.getRevenueMin());
                }
                if (result.getVoteMax()== 0 || (tuple.getVoteMax()> result.getVoteMax())) {
                    result.setVoteMax(tuple.getVoteMax());
                }if ((tuple.getVoteMin()< result.getVoteMin())) {
                    result.setVoteMin(tuple.getVoteMin());
                }
                mYear = mYear + tuple.getYear();
                count=count+1;
            }
            mYear=mYear/count;
            result.setYear((int)mYear);
            context.write(new Text(key.toString()), result);
        }
    }

    public static void main(String[] args) throws Exception {
        ProyectoMapReduce.writeFileToHDFS("/PCD2024/a_83045/ProyectoArchivo", "./resources/convert.tsv");
        UserGroupInformation ugi= UserGroupInformation.createRemoteUser("a_83045");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", "hdfs://192.168.10.1:9000");
                Job job = Job.getInstance(conf, "MpaReduce1");
                job.setJarByClass(MapReduce2.class);
                job.setMapperClass(EmployeeMinMaxCountMapper.class);
                job.setReducerClass(EmployeeMinMaxCountReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(CustomTuple.class);

                FileInputFormat.addInputPath(job,new Path("/PCD2024/a_83045/ProyectoArchivo"));
                FileOutputFormat.setOutputPath(job,new Path("/PCD2024/a_83045/ProyectoSalida"));

                boolean finalizado = job.waitForCompletion(true);
                System.out.println("Finalizado: " + finalizado);
                return null;
            }
        });
    }
}
