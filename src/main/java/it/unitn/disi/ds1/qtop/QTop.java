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