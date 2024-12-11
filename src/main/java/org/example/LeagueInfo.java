package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LeagueInfo implements WritableComparable<LeagueInfo> {

DoubleWritable wageSum;
DoubleWritable ageSum;
IntWritable wageCount;
IntWritable ageCount;
IntWritable countAll;

    public LeagueInfo() {
        set(new DoubleWritable(0), new DoubleWritable(0), new IntWritable(0), new IntWritable(0), new IntWritable(0));
    }

    public LeagueInfo(Double wageSum, Double ageSum, Integer wageCount, Integer ageCount) {
        set(new DoubleWritable(wageSum), new DoubleWritable(ageSum), new IntWritable(wageCount), new IntWritable(ageCount), new IntWritable(1));
    }

    public LeagueInfo(Double wageSum, Double ageSum, Integer wageCount, Integer ageCount, Integer countAll) {
        set(new DoubleWritable(wageSum), new DoubleWritable(ageSum), new IntWritable(wageCount), new IntWritable(ageCount), new IntWritable(countAll));
    }

    public void set(DoubleWritable wageSum, DoubleWritable ageSum, IntWritable wageCount, IntWritable ageCount, IntWritable countAll) {
        this.wageSum = wageSum;
        this.ageSum = ageSum;
        this.wageCount = wageCount;
        this.ageCount = ageCount;
        this.countAll = countAll;
    }

    public IntWritable getCountAll() {return countAll;}

    public IntWritable getWageCount() {
        return wageCount;
    }

    public IntWritable getAgeCount() {return ageCount;}

    public DoubleWritable getWageSum() { return wageSum;}

    public DoubleWritable getAgeSum() { return ageSum;}


    public void add(LeagueInfo other) {
        set(new DoubleWritable(this.wageSum.get() + other.wageSum.get()),
                new DoubleWritable(this.ageSum.get() + other.ageSum.get()),
                new IntWritable(this.wageCount.get() + other.wageCount.get()),
                new IntWritable(this.ageCount.get() + other.ageCount.get()),
                new IntWritable(this.countAll.get() + other.countAll.get()));
    }


    @Override
    public void write(DataOutput out) throws IOException {
        wageSum.write(out);
        ageSum.write(out);
        wageCount.write(out);
        ageCount.write(out);
        countAll.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        wageSum.readFields(in);
        ageSum.readFields(in);
        wageCount.readFields(in);
        ageCount.readFields(in);
        countAll.readFields(in);
    }

    @Override
    public int compareTo(LeagueInfo other) {

        int comparison = countAll.compareTo(other.countAll);

        if (comparison != 0) {
            return comparison;
        }

        comparison = wageSum.compareTo(other.wageSum);

        if (comparison != 0) {
            return comparison;
        }

        comparison = ageSum.compareTo(other.ageSum);

        if (comparison != 0) {
            return comparison;
        }

        comparison = wageCount.compareTo(other.wageCount);

        if (comparison != 0) {
            return comparison;
        }

        return ageCount.compareTo(other.ageCount);
    }

    @Override
    public String toString() {
        double avg_wage = wageSum.get() / wageCount.get();
        double avg_age = ageSum.get() / ageCount.get();
        int count = countAll.get();
        return count + "\t" + avg_wage + "\t" + avg_age;
    }
}
