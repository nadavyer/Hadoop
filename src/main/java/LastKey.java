import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LastKey implements WritableComparable<LastKey> {
    private Text key;

    @Override
    public String toString() {
        return key.toString();
    }

    public LastKey() {
        set();
    }

    public LastKey(Text key) {
        this.key = key;
        String[] parts = key.toString().split(" ");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
    }

    public String getFirstTwoWords() {
        String[] parts = key.toString().split(" ");
        return (parts[0] + " " + parts[1]);
    }

    public Double getProbability() {
        String[] parts = key.toString().split(" ");
        return (Double.parseDouble(parts[3]));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
    }

    @Override
    public int compareTo(LastKey lastKey) {
        int compeareVal = getFirstTwoWords().compareTo(lastKey.getFirstTwoWords());
        if (compeareVal == 0) {
            compeareVal = (getProbability().compareTo(lastKey.getProbability())) * (-1);
        }
        return compeareVal;
    }

    public void set() {
        this.key = new Text();
    }

    public void set(Text text) {
        this.key = text;
    }

    public Text getArgsToHash() {
        return new Text(getFirstTwoWords());
    }

    public Text getData() {
        return key;
    }
}
