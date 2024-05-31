package it.unitn.disi.ds1.qtop;

public class Controller {
    private Simulation simulation;
    private UserInterface ui;

    public Controller(Simulation simulation, UserInterface ui) {
        this.simulation = simulation;
        this.ui = ui;
    }

    public void startSimulation(int numberOfNodes, int decisionTimeout, int voteTimeout) {
        simulation.start(numberOfNodes, decisionTimeout, voteTimeout);
        clientMenu();
    }

    public void readVariable(){

    }

    public void writeVariable(){

    }

    public void crashNode() {
    }


    public void exitSimulation() {
        simulation.exit();
    }

    public void clientMenu() {
        ui.clientMenu();
    }


}
