package crystallball;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

class PairWritable implements WritableComparable<PairWritable>{
	private Text key;
	private Text value;
	
	public PairWritable(Text key, Text value) {
		this.key = key;
		this.value = value;
	}
	
	public PairWritable() {
		key = new Text();
		value = new Text();
	}
	
	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public Text getValue() {
		return value;
	}

	public void setValue(Text value) {
		this.value = value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		value.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
		
	}
	
	@Override
	public int compareTo(PairWritable o) {
		int k = this.key.toString().compareTo(o.key.toString());
		
		if (k != 0) return k;
		return this.value.toString().compareTo(o.value.toString());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		PairWritable ob = (PairWritable) obj;
		return ob.key.toString().equals(this.key.toString());
	}

	@Override
	public String toString() {
		return "(" + key + ", " + value + ")";
	}	
}
