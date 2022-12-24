package utilities;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextDoubleWritablePair implements WritableComparable {
    public Text first;
    public DoubleWritable second;

    public TextDoubleWritablePair() {
        first = new Text();
        second = new DoubleWritable();
    }

    public TextDoubleWritablePair(Text first, DoubleWritable second) {

        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public DoubleWritable getSecond() {
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
        TextDoubleWritablePair object = (TextDoubleWritablePair) o;
        String[] split = first.toString().split(" ");
        Text w1w2 = new Text(split[0] + " " + split[1]);
        split = object.first.toString().split(" ");
        Text w1w2Object = new Text(split[0] + " " + split[1]);
        int compare = w1w2.compareTo(w1w2Object);
        if (compare != 0) {
            return compare;
        }
        compare = second.compareTo(object.second);
        if (compare != 0)
            return -compare;
        return first.compareTo(object.first);

    }

    public String toString() {
        return first.toString() + "\t" + second.get();
    }
}
