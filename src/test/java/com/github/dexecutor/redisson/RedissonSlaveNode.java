package com.github.dexecutor.redisson;

import java.util.Collections;

import org.redisson.RedissonNode;
import org.redisson.config.Config;
import org.redisson.config.RedissonNodeConfig;

public class RedissonSlaveNode {

	private RedissonNode node;

	public RedissonSlaveNode(String executorName, Config config) {
		RedissonNodeConfig nodeConfig = new RedissonNodeConfig(config);
	    //nodeConfig.setExecutorServiceThreads(5);
	    nodeConfig.setExecutorServiceWorkers(Collections.singletonMap(executorName, 2));
	    node = RedissonNode.create(nodeConfig); 
	}

	public void start() {
		node.start();
	}
}
