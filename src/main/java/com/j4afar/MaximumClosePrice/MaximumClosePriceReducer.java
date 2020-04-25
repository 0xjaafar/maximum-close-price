package com.j4afar.MaximumClosePrice;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// first two param define the input the third and fourth define the output from the reducer
public class MaximumClosePriceReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
		throws IOException, InterruptedException {
		
		float maxClosePrice = Float.MIN_VALUE;
		
		// Iterate all close price and calc the max
		for (FloatWritable value : values) {
			maxClosePrice = Math.max(maxClosePrice, value.get());
		}
		
		// Write output
		context.write(key, new FloatWritable(maxClosePrice));
	}

}
