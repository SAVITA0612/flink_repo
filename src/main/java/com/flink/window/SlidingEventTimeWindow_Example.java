package com.flink.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.common.RandomNumberGeneration;

public class SlidingEventTimeWindow_Example {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setBufferTimeout(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(1);

		SlidingEventTimeWindows window = SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5));
		DataStream<Tuple2<Integer, Integer>> inputDataStream = env.addSource(new RandomNumberGeneration());
		inputDataStream.keyBy(0).window(window).maxBy(1).print();

		env.execute("SlidingEventTimeWindow example");
	}

}
