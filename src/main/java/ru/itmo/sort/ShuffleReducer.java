package ru.itmo.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ShuffleReducer extends Reducer<DoubleWritable, ShuffleData, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<ShuffleData> values, Context context) throws IOException, InterruptedException {
        for (ShuffleData value : values) {
            Text category = new Text(value.getCategory());
            context.write(category, new Text(String.format("%.2f\t%d", -1 * key.get(), value.getQuantity())));
        }
    }
}

