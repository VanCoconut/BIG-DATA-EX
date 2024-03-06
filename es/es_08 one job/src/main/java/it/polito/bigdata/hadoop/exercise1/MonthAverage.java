package it.polito.bigdata.hadoop.exercise1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MonthAverage implements org.apache.hadoop.io.Writable {
	private String month;
	private float avg = 0;


	public void setAvg(float avg) {
		this.avg = avg;
	}
	public void setMonth(String month) {
		this.month = month;
	}

	public float getAvg() {
		return avg;
	}
	public String getMonth() {
		return month;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		month = in.readLine();
		avg = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(month);
		out.writeFloat(avg);
	}

	public String toString() {

		return month+","+avg;
	}

}
