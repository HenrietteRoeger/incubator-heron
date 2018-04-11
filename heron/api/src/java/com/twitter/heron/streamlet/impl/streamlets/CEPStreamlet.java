package com.twitter.heron.streamlet.impl.streamlets;

import java.util.Set;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.SerializableBiFunction;
import com.twitter.heron.streamlet.SerializableFunction;
import com.twitter.heron.streamlet.WindowConfig;
import com.twitter.heron.streamlet.impl.StreamletImpl;
import com.twitter.heron.streamlet.impl.WindowConfigImpl;
import com.twitter.heron.streamlet.impl.StreamletImpl;
import com.twitter.heron.streamlet.impl.operators.CEPOperator;
import com.twitter.heron.streamlet.impl.operators.FilterOperator;
import com.twitter.heron.streamlet.impl.operators.MapOperator;

/**
 * CEP Streamlet represents a streamlet that contains complex events derived
 * from events in a window iff the patternMatcher found a pattern match within
 * the window. Hence, some windows might not produce any complex event if no
 * pattern match was to be found.
 * 
 * @author roegerhe
 *
 * @param <R>
 * @param <C>
 */
public class CEPStreamlet<R,S,C> extends StreamletImpl<C> {

	private StreamletImpl<R> parent;
	private SerializableBiFunction<R,S,KeyValue<C,S>> patternMatcher;
	private S state;
	private WindowConfigImpl windowCfg;

	public CEPStreamlet(StreamletImpl<R> parent, WindowConfig windowCfg, S state, SerializableBiFunction<R, S, KeyValue<C,S>> patternMatcher) {
		this.parent = parent;
		this.windowCfg = (WindowConfigImpl) windowCfg;
		this.patternMatcher = patternMatcher;
		this.state = state;
		setNumPartitions(parent.getNumPartitions());

	}

	@Override
	public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
		 setDefaultNameIfNone(StreamletNamePrefix.CEP, stageNames);
		CEPOperator<R,S,C> bolt = new CEPOperator<R,S,C>(state, patternMatcher);
		windowCfg.attachWindowConfig(bolt);
		
		bldr.setBolt(getName(), bolt, getNumPartitions()).shuffleGrouping(parent.getName()); // the shuffle grouping
																								// might become a
																								// problem for pattern
																								// // detection...
		// * CustomStreamGrouping might be an alternative! To be found in package com.twitter.heron.api.grouping;
		return true;
	}
}
