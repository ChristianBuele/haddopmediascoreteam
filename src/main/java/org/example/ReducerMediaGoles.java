package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerMediaGoles extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int totalGoles = 0;
        int totalPartidos = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(";");
            totalGoles += Integer.parseInt(parts[0]);
            totalPartidos += Integer.parseInt(parts[1]);
        }

        if (totalPartidos > 0) {
            double media = (double) totalGoles / totalPartidos;
            context.write(key, new DoubleWritable(media));
        }
    }
}
