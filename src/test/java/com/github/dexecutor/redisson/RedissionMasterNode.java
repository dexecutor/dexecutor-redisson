package com.github.dexecutor.redisson;

import java.util.concurrent.TimeUnit;

import org.redisson.Redisson;
import org.redisson.api.RScheduledExecutorService;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.Duration;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskProvider;

public class RedissionMasterNode {
	DefaultDexecutor<Integer, Integer> dexecutor;

	public RedissionMasterNode(String executorName, Config config) {
		RedissonClient redissonClient = Redisson.create(config);

		dexecutor = newTaskExecutor(executorName, redissonClient);

		buildGraph(dexecutor);
			    
	}

	public void execute() {
		dexecutor.execute(new ExecutionConfig().scheduledRetrying(4, new Duration(1, TimeUnit.SECONDS)));
	}

	private DefaultDexecutor<Integer, Integer> newTaskExecutor(String executorName, RedissonClient client) {
		RScheduledExecutorService executorService = client.getExecutorService(executorName);
		RedissonDexecutorState<Integer, Integer> dexecutorState = new RedissonDexecutorState<Integer, Integer>(executorName, client);
		DexecutorConfig<Integer, Integer> config = config(executorService, dexecutorState);
		config.setDexecutorState(dexecutorState);

		return new DefaultDexecutor<Integer, Integer>(config);
	}

	private DexecutorConfig<Integer, Integer> config(RScheduledExecutorService executorService,
			RedissonDexecutorState<Integer, Integer> dexecutorState) {
		return new DexecutorConfig<Integer, Integer>(
				new RedissonExecutionEngine<Integer, Integer>(dexecutorState, executorService), new SleepyTaskProvider());
	}
	
	private void buildGraph(final DefaultDexecutor<Integer, Integer> dexecutor) {
		dexecutor.addDependency(1, 2);
		dexecutor.addDependency(1, 2);
		dexecutor.addDependency(1, 3);
		dexecutor.addDependency(3, 4);
		dexecutor.addDependency(3, 5);
		dexecutor.addDependency(3, 6);
		dexecutor.addDependency(2, 7);
		dexecutor.addDependency(2, 9);
		dexecutor.addDependency(2, 8);
		dexecutor.addDependency(9, 10);
		dexecutor.addDependency(12, 13);
		dexecutor.addDependency(13, 4);
		dexecutor.addDependency(13, 14);
		dexecutor.addIndependent(11);
	}
	
	private static class SleepyTaskProvider implements TaskProvider<Integer, Integer> {

		public SleepyTaskProvider() {
			
		}

		public Task<Integer, Integer> provideTask(final Integer id) {

			return new Task<Integer, Integer>() {

				private static final long serialVersionUID = 1L;

				public Integer execute() {
					try {
	
						System.out.println("Executing :*****  " +  getId());
						Thread.sleep(time(0, 5000));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					return id;
				}

				private long time(int min, int max) {
					return min + (int)(Math.random() * ((max - min) + 1));
				}
			};
		}
	}

}
