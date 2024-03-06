package it.polito.bigdata.hadoop.exercise1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatisticsWritable implements org.apache.hadoop.io.Writable {
	private double sum = 0;
	private int count = 0;

	public double getSum() {
		return sum;
	}

	public void setSum(double sumValue) {
		sum = sumValue;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int countValue) {
		count = countValue;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sum = in.readDouble();
		count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(sum);
		out.writeInt(count);
	}

	public String toString() {
		String formattedString = new String("" + (double) sum / count);

		return formattedString;
	}

}
