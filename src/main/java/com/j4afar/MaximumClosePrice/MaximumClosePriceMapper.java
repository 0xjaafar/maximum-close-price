package com.j4afar.MaximumClosePrice;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MaximumClosePriceMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
	
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split(",");
		
		String stock = items[1];
		Float closePrice = Float.parseFloat(items[6]);
		
		// output from our map job
		context.write(new Text(stock), new FloatWritable(closePrice));
	}
	
}
