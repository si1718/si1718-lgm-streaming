package data.streaming.test;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AllWindowFunctionImpl implements AllWindowFunction<String, String, TimeWindow> {

	private static final long serialVersionUID = 5552908217271918246L;

	@Override
	public void apply(TimeWindow arg0, Iterable<String> arg1, Collector<String> arg2) throws Exception {
		// TODO Auto-generated method stub
		for(String s: arg1) {
			arg2.collect(s);
		}
	}

}
