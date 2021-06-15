package com.ldh.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ldh.common.AirlinePerformanceParser;
import com.ldh.counter.DelayCounters;

public class DelayCountMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable>{
	private String workType;
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		workType = context.getConfiguration().get("workType");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		
		
		if(workType.equals("departure")) {
			if(parser.isDepartureDelayAvailable()) {
				if(parser.getDepartureDelayTime() > 0) {
					context.write(outputKey, outputValue);
				} else if(parser.getDepartureDelayTime() == 0) {
					context.getCounter(DelayCounters.scheduled_departure).increment(1);
				} else if(parser.getDepartureDelayTime() < 0) {
					context.getCounter(DelayCounters.early_departure).increment(1);
				}
			} else {
				context.getCounter(DelayCounters.not_available_departure).increment(1);
			}
		} else if(workType.equals("arrival")) {
			if(parser.isArrivalDelayAvailable()) {
				if(parser.getArrivalDelayTime() > 0) {
					context.write(outputKey, outputValue);
				} else if(parser.getArrivalDelayTime() == 0) {
					context.getCounter(DelayCounters.scheduled_arrival).increment(1);
				} else if(parser.getArrivalDelayTime() < 0) {
					context.getCounter(DelayCounters.early_arrival).increment(1);
				}
			} else {
				context.getCounter(DelayCounters.not_available_arrival).increment(1);
			}
		}
	}
}
