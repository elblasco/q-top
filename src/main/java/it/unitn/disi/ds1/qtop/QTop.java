package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.qtop.Utils.StartMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static it.unitn.disi.ds1.qtop.Utils.N_NODES;

public class QTop{

    public static void main(String[] args) {

        // Create the actor system
        final ActorSystem system = ActorSystem.create("qtop");

        // Create a "virtual synchrony manager"
        ActorRef coordinator = system.actorOf(Node.props(-1, true), "coordinator");

        // Create nodes and put them to a list
        List<ActorRef> group = new ArrayList<>();
        group.add(coordinator);
        for (int i = 0; i < N_NODES - 1 ; i++) {
            group.add(system.actorOf(Node.props(i, false), "node" + i));
        }

        // Send start messages to the participants to inform them of the group
        StartMessage start = new StartMessage(group);
        for (ActorRef peer: group) {
            peer.tell(start, null);
        }

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        }
        catch (IOException ignored) {}
        system.terminate();

    }
}