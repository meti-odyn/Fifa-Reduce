package org.example;

import com.sun.istack.NotNull;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FifaMapper extends Mapper<LongWritable, Text, IntWritable, LeagueInfo> {

    private int tryParse (String input, @NotNull int onCatch) {
        try {
           return Integer.parseInt(input);
        } catch (Exception e) {
            return onCatch;
        }
    }

    private double tryParse (String input, @NotNull double onCatch) {
        try {
            return Double.parseDouble(input);
        } catch (Exception e) {
            return onCatch;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(";");

        if (data.length > 107) {

            String wageStr = data[11];
            String ageStr = data[12];
            String weightStr = data[15];
            String leagueIdStr = data[16];

            int leagueId = tryParse(leagueIdStr, -1);
            int weight = tryParse(weightStr, 101);
            if (weight > 100 || leagueId == -1) {
                return;
            }

            double age = tryParse(ageStr, -1.0);
            int ageCount = age < 0 ? 0 : 1;
            double wage = tryParse(wageStr, -1.0);
            int wageCount = wage < 0 ? 0 : 1;

            try {
                context.write(new IntWritable(leagueId), new LeagueInfo(wage, age, wageCount, ageCount));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
