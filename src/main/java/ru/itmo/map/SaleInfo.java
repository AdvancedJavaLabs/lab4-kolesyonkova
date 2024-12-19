package ru.itmo.map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SaleInfo implements Writable {
    private double price;
    private int quantity;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(price);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        price = in.readDouble();
        quantity = in.readInt();
    }
}
