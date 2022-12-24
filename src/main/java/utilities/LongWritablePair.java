package utilities;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongWritablePair implements WritableComparable {
    public LongWritable first;
    public LongWritable second;

    public LongWritablePair() {
        first = new LongWritable();
        second = new LongWritable();
    }

    public LongWritablePair(LongWritable first, LongWritable second) {
        this.first = first;
        this.second = second;
    }

    public LongWritable getFirst() {
        return first;
    }

    public LongWritable getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }


    @Override
    public int compareTo(Object o) {
        LongWritablePair object = (LongWritablePair) o;
        int compareLong = second.compareTo(object.second);
        if (compareLong != 0) {
            return compareLong;
        }
        return first.compareTo(object.first);
    }

    public String toString() {
        return first.get() + "\t" + second.get();
    }
}
