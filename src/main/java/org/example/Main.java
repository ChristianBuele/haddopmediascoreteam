package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 2) {
            System.err.println("Uso: MediaGolesPartido <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Media de Goles por Partido");

        job.setJarByClass(Main.class);

        // Clases de Mapper, Combiner y Reducer
        job.setMapperClass(Mapper.class);
        job.setCombinerClass(CombinerMediaGoles.class);
        job.setReducerClass(ReducerMediaGoles.class);

        // Tipos de salida del Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Tipos de salida final
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Archivos de entrada/salida
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Salida ordenada alfabéticamente
        job.setSortComparatorClass(Text.Comparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}