package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable outValue = new IntWritable();
    private Text team = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
        try {
            String row = value.toString();
            if (row.isEmpty() || row.contains(",Season")) {
                return; // Skip empty lines and first row
            }
            String[] data = row.split(",");
            if (data.length < 7) {
                return; // Skip rows with insufficient data
            }
            String localTeam = data[5].trim();
            String visitorTeam = data[6].trim();
            Integer[] scores = goals(data[3].trim());
            Integer localScore = scores[0];
            Integer visitorScore = scores[1];
            team.set(localTeam+";Local");
            outValue.set(localScore);
            context.write(team, outValue);

            team.set(visitorTeam+";Visitante");
            outValue.set(visitorScore);
            context.write(team, outValue);
        } catch (Exception e) {
            System.out.println("Error processing row: " + value.toString());
        }


    }
    private Integer[] goals(String scoreData){
        String[] goals = scoreData.trim().split("-");
        Integer[] scores = new Integer[2];
        if (goals.length == 2) {
            try {
                scores[0] = Integer.parseInt(goals[0].trim());
                scores[1] = Integer.parseInt(goals[1].trim());
                return scores;
            } catch (NumberFormatException e) {
                System.out.println("Invalid score format: " + scoreData);
            }
        } else {
            System.out.println("Goals number incorrect: " + goals.length);
        }
        throw new IllegalArgumentException();
    }
}
