package utilities;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongTextWritablePair implements WritableComparable {
    public LongWritable first;
    public Text[] second;

    public LongTextWritablePair() {
        first = new LongWritable();
        second = new Text[1];
        second[0] = new Text();

    }

    public LongTextWritablePair(LongWritable first, Text[] second) {
        this.first = first;
        this.second = second;
    }

    public LongWritable getFirst() {
        return first;
    }

    public Text[] getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        for (Text text : second) {
            text.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        for (Text text : second) {
            text.readFields(dataInput);
        }
    }

    @Override
    public int compareTo(Object o) {
        LongTextWritablePair object = (LongTextWritablePair) o;
        int compareLong = first.compareTo(object.first);
        if (compareLong != 0) {
            return compareLong;
        }

        return second[0].compareTo(object.second[0]);
    }

    public String toString() {
        return first.get() + "\t" + second[0].toString();
    }
}
