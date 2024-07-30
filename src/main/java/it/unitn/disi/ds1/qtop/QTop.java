package it.unitn.disi.ds1.qtop;

/**
 * Main class to start the simulation.
 */
public class QTop{

    public static void main(String[] args) {
        //Initializes the simulation
        Simulation simulation = new Simulation();

        //User interface and client interface
        UserInterface ui = new UserInterface(null);

        //Object to handle ui commands
        Controller controller = new Controller(simulation, ui);

        //set the controller to handle callbacks
        ui.setController(controller);
        ui.start();
    }
}