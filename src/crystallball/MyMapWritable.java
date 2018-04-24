package crystallball;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class MyMapWritable extends MapWritable {

	public MyMapWritable() {
		super();
	}

	@Override
	public String toString() {
		StringBuilder out = new StringBuilder();
		out.append("{");
		
		int i = 1;
		for (Entry<Writable, Writable> entry : this.entrySet()) {
			out.append(String.format("(%s, %s)", entry.getKey().toString(), entry
					.getValue().toString()));
			if (i < this.size()) {
				out.append(", ");
				i++;
			}
		}
		out.append("}");
		return out.toString();
	}
	
	
	
}