package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Simulation {
    private final ActorSystem system;
    private List<ActorRef> group;

    private final Logger logger = Logger.getInstance();

    public Simulation() {
        group = new ArrayList<>();
        system = ActorSystem.create("qtop");
    }

    public void start(int numberOfNodes, int decisionTimeout, int voteTimeout) {

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
