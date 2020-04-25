package com.j4afar.MaximumClosePrice;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MaximumClosePriceMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
	
	// Used for counters
	public enum Volume {
		HIGH_VOLUME,
		LOW_VOLUME
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split(",");
	
		String stock = items[1];
		Float closePrice = Float.parseFloat(items[6]);
		
		// Used for counters
		int volume = Integer.parseInt(items[7]);
		if (volume >= 500000)
			context.getCounter(Volume.HIGH_VOLUME).increment(1);
		else
			context.getCounter(Volume.LOW_VOLUME).increment(1);
		
		
		// output from our map job
		context.write(new Text(stock), new FloatWritable(closePrice));
	}
	
}
