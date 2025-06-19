package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String row = value.toString();

        if (key.get() == 0 && row.contains("Season")) {
            return; // Omitir cabecera
        }

        String[] data = row.split(",");
        if (data.length < 7) {
            return; // Omitir líneas incompletas
        }

        try {
            String homeTeam = data[5].trim();
            String awayTeam = data[6].trim();
            String[] goals = data[3].trim().split("-");

            int homeGoals = Integer.parseInt(goals[0].trim());
            int awayGoals = Integer.parseInt(goals[1].trim());

            // Emitir equipo local
            outKey.set(homeTeam + ";Local");
            outValue.set(homeGoals + ";1");
            context.write(outKey, outValue);

            // Emitir equipo visitante
            outKey.set(awayTeam + ";Visitante");
            outValue.set(awayGoals + ";1");
            context.write(outKey, outValue);
        } catch (Exception e) {
            // Manejo de errores mínimo
        }

    }
}