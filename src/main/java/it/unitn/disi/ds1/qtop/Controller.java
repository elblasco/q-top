package it.unitn.disi.ds1.qtop;

/**
 * Controller for the simulation.
 */
public class Controller {
	private static final Logger LOGGER = Logger.getInstance();
	private final Simulation simulation;
	private final UserInterface ui;

	/**
	 * Constructor for the Controller.
	 *
	 * @param simulation the simulation
	 * @param ui         the user interface
	 */
	public Controller(Simulation simulation, UserInterface ui) {
		this.simulation = simulation;
		this.ui = ui;
	}

	/**
	 * Start the simulation (network), with the given parameters.
	 *
	 * @param numberOfNodes   number of nodes
	 * @param numberOfClients number of clients
	 * @param voteTimeout     vote timeout
	 * @param writeTimeout    write timeout
	 */
	public void startSimulation(int numberOfNodes, int numberOfClients, int voteTimeout, int writeTimeout) {
		simulation.start(
				numberOfNodes,
				numberOfClients,
				voteTimeout,
				writeTimeout
		);
		clientMenu();
	}

	/**
	 * Make crash a Node.
	 *
	 * @param crashType the type of crash
	 */
	public void crashNode(int crashType) {
		LOGGER.log(
				Utils.LogLevel.INFO,
				"[SYSTEM] inserting crash type " + crashType
		);
		this.simulation.addCrashNode(crashType);
	}

	/**
	 * Quit.
	 */
	public void exitSimulation() {
		simulation.exit();
	}

	/**
	 * Print the menu to tty.
	 */
	public void clientMenu() {
		ui.clientMenu();
	}
}
