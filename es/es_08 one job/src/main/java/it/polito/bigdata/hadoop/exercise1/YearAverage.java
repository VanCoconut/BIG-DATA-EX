package it.polito.bigdata.hadoop.exercise1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearAverage implements org.apache.hadoop.io.Writable {
	private String year;
	private float avg = 0;


	public void setAvg(float avg) {
		this.avg = avg;
	}
	public void setYear(String year) {
		this.year = year;
	}

	public float getAvg() {
		return avg;
	}
	public String getYear() {
		return year;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = in.readLine();
		avg = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(year);
		out.writeFloat(avg);
	}

	public String toString() {

		return year+","+avg;
	}

}
