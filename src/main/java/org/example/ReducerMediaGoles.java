package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class ReducerMediaGoles extends Reducer<Text, IntWritable, Text, Text> {

    private Text outputValue = new Text();
    private DecimalFormat df = new DecimalFormat("#.00");

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException  {
        int totalGoals = 0;
        int count = 0;

        for (IntWritable val : values) {
            totalGoals += val.get();
            count++;
        }

        double average = (double) totalGoals / count;
        outputValue.set(df.format(average));
        context.write(key, outputValue);
    }
}
