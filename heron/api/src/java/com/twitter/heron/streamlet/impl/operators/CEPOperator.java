package com.twitter.heron.streamlet.impl.operators;

import java.util.Map;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.windowing.TupleWindow;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.SerializableBiFunction;
import com.twitter.heron.streamlet.SerializablePredicate;

/**
 * An operator that detects a complex event in a window and emits it iff found.
 * Breaks as soon as the first event is found! 
 * else nothing.
 * 
 * @author roegerhe
 *
 */
public class CEPOperator<R,S,C> extends StreamletWindowOperator {
	private static final long serialVersionUID = -4748646871471052706L;
	private SerializableBiFunction<R, S, KeyValue<C,S>> patternMatcher;
	S state;
	private OutputCollector collector;

	public CEPOperator(S initialState, SerializableBiFunction<R, S, KeyValue<C,S>> patternMatcher) {
		this.state = initialState;
		this.patternMatcher = patternMatcher;
	}

	@Override
	public void prepare(Map<String, Object> heronConf, TopologyContext context, OutputCollector outputCollector) {
		collector = outputCollector;

	}

	@Override
	public void execute(TupleWindow inputWindow) {
		for (Tuple tuple : inputWindow.get()) {
			R tup = (R) tuple.getValue(0);
			KeyValue<C,S> out = patternMatcher.apply(tup, state); //might have more than one element. then the first is the output, the second the state.
			if (out.getKey() != null) {
				collector.emit(new Values(out));
				return; //break if first pattern is found.
			}
			else {
				state = out.getValue(); //update state.
			}
		}
	}

}
