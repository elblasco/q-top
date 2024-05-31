package it.unitn.disi.ds1.qtop;

import java.util.Scanner;

public class UserInterface {
    private Controller controller;

    public UserInterface(Controller controller) {
        this.controller = controller;
    }

    public void start() {

        //default values
        int numberOfNodes = 5;
        int decisionTimeout = 2000;
        int voteTimeout = 1000;
        int crashPoint = 0;

        System.out.println("Qtop - DS Project 2023/2024 - Blascovich Alessio, Cereser Lorenzo \n");

        System.out.println("""
                This is a simulation of 2PC + token ring leader election for the\s
                distributed system course, the system is composed by a number of\s
                nodes that can be set by the user. Virtual crashes can be inserted
                at user discretion but only in one place at a time. Timeouts can\s
                also be set.\s""");
        System.out.println("_________________________________________________________________");
        Scanner scanner = new Scanner(System.in);
        int input = 0;
        while (input != 1) {
            System.out.println("Please select an option:");
            System.out.println("_________________________________________________________________");
            System.out.println("1. Start the simulation");
            System.out.println("2. Set the number of nodes [current: "+numberOfNodes+"]");
            System.out.println("3. Set the decision timeout in ms, [current: "+decisionTimeout+"ms]");
            System.out.println("4. Set the vote timeout in ms, [current: "+voteTimeout+"ms]");
            System.out.println("5. Select virtual crash position");
            System.out.println("_________________________________________________________________");

            input = scanner.nextInt();

            switch (input) {
                case 1:
                    controller.startSimulation(numberOfNodes, decisionTimeout, voteTimeout);
                    break;
                case 2:
                    System.out.println("Insert the number of nodes:");
                    numberOfNodes = scanner.nextInt();
                    break;
                case 3:
                    System.out.println("Insert the decision timeout in ms:");
                    decisionTimeout = scanner.nextInt();
                    break;
                case 4:
                    System.out.println("Insert the vote timeout in ms:");
                    voteTimeout = scanner.nextInt();
                    break;
                default:
                    System.out.println("Invalid option, please try again");
                    break;
            }
        }

    }
}
