package ru.itmo.map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaleReducer extends Reducer<Text, SaleInfo, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<SaleInfo> values, Context context) throws IOException, InterruptedException {
        double totalPrice = 0.0;
        int totalQuantity = 0;

        for (SaleInfo val : values) {
            totalPrice += val.getPrice();
            totalQuantity += val.getQuantity();
        }

        context.write(key, new Text(String.format("%.2f\t%d", totalPrice, totalQuantity)));
    }
}

