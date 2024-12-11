package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FifaReducer extends Reducer<IntWritable, LeagueInfo, Text, Text> {

    public void reduce(IntWritable key, Iterable<LeagueInfo> values,
                       Context context) throws IOException, InterruptedException {

        Text resultKey = new Text(key.toString());//new Text(key.toString() + '\t')
        LeagueInfo leagueInfo = new LeagueInfo();

        for (LeagueInfo value : values) {
            leagueInfo.add(value);
        }
        context.write(resultKey, new Text(leagueInfo.toString()));
    }
}
