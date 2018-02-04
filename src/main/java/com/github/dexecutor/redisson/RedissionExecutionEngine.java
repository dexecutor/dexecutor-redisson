package com.github.dexecutor.redisson;

import java.util.concurrent.ExecutionException;

import org.redisson.api.RScheduledExecutorService;
import org.redisson.executor.RedissonCompletionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dexecutor.core.DexecutorState;
import com.github.dexecutor.core.ExecutionEngine;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskExecutionException;

public class RedissionExecutionEngine<T, R> implements ExecutionEngine<T, R> {

	private static final Logger logger = LoggerFactory.getLogger(RedissionExecutionEngine.class);

	private RScheduledExecutorService executorService;
	private RedissonCompletionService<ExecutionResult<T, R>> completionService;

	private final DexecutorState<T, R> dexecutorState;
	
	public RedissionExecutionEngine(DexecutorState<T, R> dexecutorState, RScheduledExecutorService rExecutorService) {
		this.dexecutorState = dexecutorState;
		this.executorService = rExecutorService;
		this.completionService = new RedissonCompletionService<ExecutionResult<T, R>>(executorService);
	}

	@Override
	public void submit(Task<T, R> task) {
		logger.debug("Received Task {} ", task.getId());
		this.completionService.submit(new SerializableCallable<T, R>(task));		
	}

	@Override
	public ExecutionResult<T, R> processResult() throws TaskExecutionException {
		ExecutionResult<T, R> executionResult;
		try {
			executionResult = completionService.take().get();
			if (executionResult.isSuccess()) {
				this.dexecutorState.removeErrored(executionResult);
			} else {
				this.dexecutorState.addErrored(executionResult);
			}
			return executionResult;
		} catch (InterruptedException | ExecutionException e) {
			throw new TaskExecutionException("Task interrupted");
		}
	}

	@Override
	public boolean isDistributed() {
		return true;
	}

	@Override
	public boolean isAnyTaskInError() {
		return this.dexecutorState.erroredCount() > 0;
	}

	@Override
	public String toString() {
		return this.executorService.toString();
	}
}
