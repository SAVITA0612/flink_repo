package com.common;

import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RandomNumberGeneration implements SourceFunction<Tuple2<Integer, Integer>> {
	private static final long serialVersionUID = 1L;
	int BOUND = 1000;
	private volatile boolean isRunning = true;
	private int counter = 0;
	private Random random = new Random();

	public void cancel() {
		isRunning = false;
	}

	public void run(SourceFunction.SourceContext<Tuple2<Integer, Integer>> arg0) throws Exception {
		while(isRunning && counter < BOUND){
			int first = counter+1;
			int second = random.nextInt(BOUND/2-1)-1;
			arg0.collect(Tuple2.of(first, second));
			System.out.println("first : "+ first + " second : " +second);
			counter++;
			Thread.sleep(5000L);
		}

	}

}
