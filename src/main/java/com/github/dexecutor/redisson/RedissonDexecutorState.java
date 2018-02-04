package com.github.dexecutor.redisson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;

import com.github.dexecutor.core.DexecutorState;
import com.github.dexecutor.core.Phase;
import com.github.dexecutor.core.graph.Dag;
import com.github.dexecutor.core.graph.DefaultDag;
import com.github.dexecutor.core.graph.Node;
import com.github.dexecutor.core.graph.Traversar;
import com.github.dexecutor.core.graph.TraversarAction;
import com.github.dexecutor.core.graph.Validator;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.ExecutionResults;

public class RedissonDexecutorState<T, R> implements DexecutorState<T, R> {

	private RAtomicLong nodesCount;
	private RList<Node<T, R>> processedNodes;
	private RList<Node<T, R>> discontinuedNodes;
	private RList<ExecutionResult<T, R>> erroredNodes;
	private RBucket<Dag<T, R>> dagBucket;
	private RBucket<Phase> currentPhase;

	public RedissonDexecutorState(String prefix, RedissonClient client) {
		this.nodesCount = client.getAtomicLong(prefix + "nodesCount");
		this.processedNodes = client.getList(prefix + "processedNodes");
		this.discontinuedNodes = client.getList(prefix + "discontinuedNodes");
		this.erroredNodes = client.getList(prefix + "erroredNodes");
		this.dagBucket = client.getBucket(prefix + "dagBucket");
		this.dagBucket.set(new DefaultDag<T, R>());
		this.currentPhase = client.getBucket(prefix + "currentPhase"); 
		
		if (currentPhase.get() == null) {			
			this.currentPhase.set(Phase.BUILDING);
		}
	}

	private Dag<T, R> getDag() {
		return this.dagBucket.get();
	}

	@Override
	public void addIndependent(T nodeValue) {
		Dag<T, R> graph = getDag();
		graph.addIndependent(nodeValue);
		this.dagBucket.set(graph);
	}

	@Override
	public void addDependency(T evalFirstValue, T evalAfterValue) {
		Dag<T, R> graph = getDag();
		graph.addDependency(evalFirstValue, evalAfterValue);
		this.dagBucket.set(graph);
		
	}

	@Override
	public void addAsDependentOnAllLeafNodes(T nodeValue) {
		Dag<T, R> graph = getDag();
		graph.addAsDependentOnAllLeafNodes(nodeValue);
		this.dagBucket.set(graph);		
	}

	@Override
	public void addAsDependencyToAllInitialNodes(T nodeValue) {
		Dag<T, R> graph = getDag();
		graph.addAsDependencyToAllInitialNodes(nodeValue);
		this.dagBucket.set(graph);
	}

	@Override
	public Node<T, R> getGraphNode(T id) {
		return this.getDag().get(id);
	}

	@Override
	public int graphSize() {
		return this.getDag().size();
	}

	@Override
	public Set<Node<T, R>> getInitialNodes() {
		return this.getDag().getInitialNodes();
	}

	@Override
	public Set<Node<T, R>> getNonProcessedRootNodes() {
		return this.getDag().getNonProcessedRootNodes();
	}

	@Override
	public void print(Traversar<T, R> traversar, TraversarAction<T, R> action) {
		traversar.traverse(this.getDag(), action);		
	}

	@Override
	public void validate(Validator<T, R> validator) {
		validator.validate(this.getDag());		
	}

	@Override
	public void setCurrentPhase(Phase currentPhase) {
		this.currentPhase.set(currentPhase);		
	}

	@Override
	public Phase getCurrentPhase() {
		return this.currentPhase.get();
	}

	@Override
	public int getUnProcessedNodesCount() {
		return (int) this.nodesCount.get();
	}

	@Override
	public void incrementUnProcessedNodesCount() {
		this.nodesCount.incrementAndGet();		
	}

	@Override
	public void decrementUnProcessedNodesCount() {
		this.nodesCount.decrementAndGet();		
	}

	@Override
	public boolean shouldProcess(Node<T, R> node) {
		return !isAlreadyProcessed(node) && allIncomingNodesProcessed(node);
	}
	
	private boolean isAlreadyProcessed(final Node<T, R> node) {
		return this.processedNodes.contains(node);
	}

	private boolean allIncomingNodesProcessed(final Node<T, R> node) {
		if (node.getInComingNodes().isEmpty() || areAlreadyProcessed(node.getInComingNodes())) {
			return true;
		}
		return false;
	}

	private boolean areAlreadyProcessed(final Set<Node<T, R>> nodes) {
        return this.processedNodes.containsAll(nodes);
    }

	@Override
	public void markProcessingDone(Node<T, R> node) {
		this.processedNodes.add(node);
		
	}

	@Override
	public Collection<Node<T, R>> getProcessedNodes() {
		return new ArrayList<>(this.processedNodes);
	}

	@Override
	public boolean isDiscontinuedNodesNotEmpty() {
		return !this.discontinuedNodes.isEmpty();
	}

	@Override
	public Collection<Node<T, R>> getDiscontinuedNodes() {
		return new ArrayList<Node<T, R>>(this.discontinuedNodes);
	}

	@Override
	public void markDiscontinuedNodesProcessed() {
		this.discontinuedNodes.clear();
	}

	@Override
	public void processAfterNoError(Collection<Node<T, R>> nodes) {
		this.discontinuedNodes.addAll(nodes);		
	}

	@Override
	public void addErrored(ExecutionResult<T, R> task) {
		this.erroredNodes.add(task);		
	}

	@Override
	public void removeErrored(ExecutionResult<T, R> task) {
		this.erroredNodes.remove(task);
	}

	@Override
	public int erroredCount() {
		return this.erroredNodes.size();
	}

	@Override
	public ExecutionResults<T, R> getErrored() {
		ExecutionResults<T, R> result = new ExecutionResults<>();
		for (ExecutionResult<T, R> r : this.erroredNodes) {
			result.add(r);
		}
		return result;
	}

	@Override
	public void forcedStop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onTerminate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onRecover() {
		// TODO Auto-generated method stub
		
	}

}
