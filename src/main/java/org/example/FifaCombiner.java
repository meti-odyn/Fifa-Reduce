package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FifaCombiner extends Reducer<IntWritable, LeagueInfo, IntWritable, LeagueInfo> {

    public void reduce(IntWritable key, Iterable<LeagueInfo> values,
                       Context context) throws IOException, InterruptedException {
        LeagueInfo leagueInfo = new LeagueInfo();
        for (LeagueInfo value : values) {
            leagueInfo.add(value);
        }
        context.write(key, leagueInfo);
    }
}
