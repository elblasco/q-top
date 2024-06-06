package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Simulation {
    private final ActorSystem system;
    private List<ActorRef> group;
    private int numberOfNodes;
    private HashSet<Integer> crashedNodes = new HashSet<Integer>();

    private final Logger logger = Logger.getInstance();

    public Simulation() {
        group = new ArrayList<>();
        system = ActorSystem.create("qtop");
    }

    public void start(int numberOfNodes, int decisionTimeout, int voteTimeout) {
        // Set initial number of nodes
        this.numberOfNodes = numberOfNodes;

        // Create a "virtual synchrony manager"
        ActorRef coordinator = system.actorOf(Coordinator.props(0,numberOfNodes,decisionTimeout,voteTimeout), "coordinator");

        // Create nodes and put them to a list
        group.add(coordinator);
        for (int i = 1; i < numberOfNodes ; i++) {
            group.add(system.actorOf(Receiver.props(i, coordinator, decisionTimeout,voteTimeout), "node" + i));
        }

        // Send start messages to the participants to inform them of the group
        Utils.StartMessage start = new Utils.StartMessage(group);
        for (ActorRef peer: group) {
            peer.tell(start, null);
        }

        logger.log(Utils.LogLevel.INFO, "Simulation started with " + numberOfNodes + " nodes, decision timeout: " + decisionTimeout + "ms, vote timeout: " + voteTimeout + "ms");

    }

    public int getNumberOfNodes() {
        return this.numberOfNodes;
    }

    public boolean isNodeCrashed(int nodeId) {
        return this.crashedNodes.contains(nodeId);
    }

    public void addCrashNode(int nodeId, int crashType) {
        Utils.CrashType crashReason = switch (crashType)
        {
            case 1 -> Utils.CrashType.NODE_BEFORE_WRITE_REQUEST;
            case 2 -> Utils.CrashType.NODE_AFTER_WRITE_REQUEST;
            case 3 -> Utils.CrashType.NODE_AFTER_VOTE_REQUEST;
            case 4 -> Utils.CrashType.NODE_BEFORE_VOTE_CAST;
            case 5 -> Utils.CrashType.COORDINATOR_BEFORE_RW_REQUEST;
            case 6 -> Utils.CrashType.COORDINATOR_AFTER_RW_REQUEST;
            case 7 -> Utils.CrashType.COORDINATOR_NO_QUORUM;
            case 8 -> Utils.CrashType.COORDINATOR_QUORUM;
            default -> Utils.CrashType.NO_CRASH;
        };
        this.group.get(nodeId).tell(
                new Utils.CrashRequest(crashReason),
                null
        );
        this.crashedNodes.add(nodeId);
    }

    public void exit() {
        logger.log(Utils.LogLevel.INFO, "Simulation terminated");
        system.terminate();
    }

    public void readVariable(int node) {

        logger.log(Utils.LogLevel.INFO, "[CLIENT] Requesting the value of the shared variable from node " + node);
    }

    public void writeVariable(int node, int value) {

        logger.log(Utils.LogLevel.INFO, "[CLIENT] Requesting to write the value " + value + " to the shared variable from node " + node);
    }
}
