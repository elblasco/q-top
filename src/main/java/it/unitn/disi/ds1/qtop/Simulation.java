package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Simulation {
    private final ActorSystem system;
    private List<ActorRef> group;

    public Simulation() {
        group = new ArrayList<>();
        system = ActorSystem.create("qtop");
    }

    public void start(int numberOfNodes, int decisionTimeout, int voteTimeout) {

        // Create a "virtual synchrony manager"
        ActorRef coordinator = system.actorOf(Coordinator.props(0), "coordinator");

        // Create nodes and put them to a list
        group.add(coordinator);
        for (int i = 1; i < Utils.N_NODES ; i++) {
            group.add(system.actorOf(Receiver.props(i, coordinator), "node" + i));
        }

        // Send start messages to the participants to inform them of the group
        Utils.StartMessage start = new Utils.StartMessage(group);
        for (ActorRef peer: group) {
            peer.tell(start, null);
        }


    }


    public void exit() {
        system.terminate();
    }
}
