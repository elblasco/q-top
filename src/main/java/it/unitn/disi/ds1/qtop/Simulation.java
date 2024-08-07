package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Simulation class layer between the UserInterface and the network.
 */
public class Simulation {
	private final ActorSystem system;
	private final List<ActorRef> group;
	private int numberOfNodes;

	private final Logger logger = Logger.getInstance();

	/**
	 * Create a new simulation.
	 */
	public Simulation() {
		group = new ArrayList<>();
		system = ActorSystem.create("qtop");
	}

	/**
	 * Start the network with the given parameters.
	 *
	 * @param numberOfNodes   number of nodes
	 * @param numberOfClients number of clients
	 * @param voteTimeout     vote timeout
	 * @param writeTimeout    write timeout
	 */
	public void start(int numberOfNodes, int numberOfClients, int voteTimeout, int writeTimeout) {
		// Set initial number of nodes
		this.numberOfNodes = numberOfNodes;
		// Create a "virtual synchrony manager"
		ActorRef coordinator = system.actorOf(
				Node.props(
						null,
						0,
						voteTimeout,
						writeTimeout,
						0,
						numberOfNodes
				),
				"coordinator"
		);
		// Create nodes and put them to a list
		group.add(coordinator);
		for (int i = 1; i < numberOfNodes; i++)
		{
			group.add(system.actorOf(
					Node.props(
							coordinator,
							i,
							voteTimeout,
							writeTimeout,
							numberOfNodes * Utils.ELECTION_TIMEOUT * 2,
							numberOfNodes
					),
					"node" + i
			));
		}
		for (int i = 0; i < numberOfClients; i++)
		{
			group.add(system.actorOf(
					Client.props(
							numberOfNodes + i,
							group,
							numberOfNodes
					),
					"client" + numberOfNodes + i
			));
		}
		// Send start messages to the participants to inform them of the group
		Utils.StartMessage start = new Utils.StartMessage(group);
		for (ActorRef peer : group)
		{
			peer.tell(
					start,
					null
			);
		}
		logger.log(
				Utils.LogLevel.INFO,
				"Simulation started with " + numberOfNodes + " nodes, vote timeout: " + voteTimeout + "ms"
		);

	}

	/**
	 * Add a crash to the network.
	 *
	 * @param crashType the type of crash
	 */
	public void addCrashNode(int crashType) {
		int clientId = new Random().nextInt(this.group.size() - this.numberOfNodes) + this.numberOfNodes;
		Utils.CrashType crashReason = switch (crashType)
		{
			case 1 -> Utils.CrashType.NODE_BEFORE_WRITE_REQUEST;
			case 2 -> Utils.CrashType.NODE_AFTER_WRITE_REQUEST;
			case 3 -> Utils.CrashType.NODE_AFTER_VOTE_REQUEST;
			case 4 -> Utils.CrashType.NODE_AFTER_VOTE_CAST;
			case 5 -> Utils.CrashType.COORDINATOR_ON_VOTE_REQUEST;
			case 6 -> Utils.CrashType.COORDINATOR_ON_DECISION_RESPONSE;
			default -> Utils.CrashType.NO_CRASH;
		};
		// Crash messages are instantaneous
		this.group.get(clientId).tell(
				new Utils.CrashRequest(crashReason),
				null
		);
	}

	/**
	 * Shut down the network.
	 */
	public void exit() {
		logger.log(
				Utils.LogLevel.INFO,
				"Simulation terminated"
		);
		system.terminate();
	}
}
