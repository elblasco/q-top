package it.unitn.disi.ds1.qtop;

public class Controller {
	private static final Logger LOGGER = Logger.getInstance();
	private final Simulation simulation;
	private final UserInterface ui;

    public Controller(Simulation simulation, UserInterface ui) {
        this.simulation = simulation;
        this.ui = ui;
    }

	public void startSimulation(int numberOfNodes, int numberOfClients, int decisionTimeout, int voteTimeout,
			int writeTimeout) {
		simulation.start(
				numberOfNodes,
				numberOfClients,
				decisionTimeout,
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
	 *
	 */
    public void clientMenu() {
        ui.clientMenu();
    }
}
