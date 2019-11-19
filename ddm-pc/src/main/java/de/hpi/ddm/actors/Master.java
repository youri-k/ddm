package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";

    public static Props props(final ActorRef reader, final ActorRef collector) {
        return Props.create(Master.class, () -> new Master(reader, collector));
    }

    public Master(final ActorRef reader, final ActorRef collector) {
        this.reader = reader;
        this.collector = collector;
        this.workers = new HashMap<>();
        this.hashedHints = new HashMap<>();
        this.solvedHints = new HashMap<>();
        this.hashedPws = new HashMap<>();
        this.tasks = new Stack<>();
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = -50374816448627600L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data
    public static class ReadyMessage implements Serializable {
        private static final long serialVersionUID = -90374816448627606L;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SolvedHintMessage implements Serializable {
        private static final long serialVersionUID = -3303081601689723392L;
        String hint;
        String hash;
        int[] rows;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SolvedCrackMessage implements Serializable {
        private static final long serialVersionUID = -4303081601689723395L;
        int row;
        String PW;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final Map<ActorRef, Worker.WorkMessage> workers;

    private long startTime;

    private final Stack<Worker.WorkMessage> tasks;
    private final Map<String, List<Integer>> hashedHints;
    private final Map<Integer, List<String>> solvedHints;
    private final Map<Integer, String> hashedPws;
    private int passwordLength;
    private char[] characterUniverse;
    private int numHints;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(RegistrationMessage.class, this::handle)
                .match(SolvedHintMessage.class, this::handle)
                .match(SolvedCrackMessage.class, this::handle)
                .match(ReadyMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();

        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    protected void handle(BatchMessage message) {

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        // The input file is read in batches for two reasons: /////////////////////////////////////////////////
        // 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
        // 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
        // TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        if (message.getLines().isEmpty()) return;

        this.log().info("Got a new batch of work of size " + message.getLines().size());

        this.solvedHints.clear();

        String[] lineOne = message.getLines().get(0);
        this.characterUniverse = lineOne[2].toCharArray();
        this.passwordLength = Integer.parseInt(lineOne[3]);
        this.numHints = lineOne.length - 5;

        for (String[] line : message.getLines()) {
            int rowId = Integer.parseInt(line[0]);
            this.hashedPws.put(rowId, line[4]);
            for (int i = 5; i < line.length; i++) {
                String hashedHint = line[i];
                List<Integer> currentRows = this.hashedHints.get(hashedHint);

                if (currentRows == null) currentRows = new ArrayList<>();

                currentRows.add(rowId);
                this.hashedHints.put(hashedHint, currentRows);
            }
        }

        for (char c : this.characterUniverse) {
            this.tasks.add(new Worker.WorkMessage(ArrayUtils.removeElement(characterUniverse, c), ArrayUtils.EMPTY_CHAR_ARRAY));
        }

        for (Map.Entry<ActorRef, Worker.WorkMessage> worker : this.workers.entrySet()) {
            if (worker.getValue() == null) {
                worker.getKey().tell(new Worker.HintMessage(this.hashedHints), this.self());
                Worker.WorkMessage currWork = this.tasks.pop();
                worker.getKey().tell(currWork, this.self());
                this.workers.put(worker.getKey(), currWork);
            }
        }

        // this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
        // this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    private void handle(ReadyMessage readyMessage) {
        this.log().info("Received ready message from worker " + this.sender().toString());
        this.sender().tell(new Worker.HintMessage(this.hashedHints), this.self());

        if(!this.tasks.isEmpty()) {
            Worker.WorkMessage currWork = this.tasks.pop();
            this.sender().tell(currWork, this.self());
            this.workers.put(this.sender(), currWork);
        } else {
            // TODO split work
            this.workers.put(this.sender(), null);
        }
    }

    protected void handle(SolvedHintMessage message) {
        // TODO Propagate usedHashes map on Key found

        this.log().info("Solved Hint " + message.getHint());

        for (int i : message.getRows()) {
            List<String> currentHints = this.solvedHints.get(i);
            if (currentHints == null) currentHints = new ArrayList<>();

            currentHints.add(message.getHint());
            this.solvedHints.put(i, currentHints);
            if (currentHints.size() == this.numHints)
                this.sender().tell(new Worker.CrackMessage(i, this.hashedPws.get(i), currentHints.toArray(new String[0]), this.characterUniverse, this.passwordLength), this.self());
        }

        this.hashedHints.remove(message.getHash());
        if (this.hashedHints.isEmpty())
            this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    protected void handle(SolvedCrackMessage message) {
        this.log().info("Cracked PW of line " + message.getRow() + ", it's :" + message.getPW());
        this.collector.tell(new Collector.CollectMessage(message.getPW()), this.self());
        this.hashedPws.remove(message.getRow());

        if(hashedPws.isEmpty()){
            this.collector.tell(new Collector.PrintMessage(), this.self());
            this.terminate();
        }
    }

    protected void terminate() {
        this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());

        for (ActorRef worker : this.workers.keySet()) {
            this.context().unwatch(worker);
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }

        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

        long executionTime = System.currentTimeMillis() - this.startTime;
        this.log().info("Algorithm finished in {} ms", executionTime);
    }

    protected void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.sender().tell(new Worker.HintMessage(this.hashedHints), this.self());
        if (!this.tasks.isEmpty()) {
            Worker.WorkMessage currWork = this.tasks.pop();
            this.sender().tell(currWork, this.self());
            this.workers.put(this.sender(), currWork);
        } else {
            this.workers.put(this.sender(), null);
            // TODO split work
        }

        this.log().info("Registered {}", this.sender());
    }

    protected void handle(Terminated message) {
        Worker.WorkMessage unfinishedWork = this.workers.get(message.getActor());
        this.tasks.add(unfinishedWork);

        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
        this.log().info("Unregistered {}", message.getActor());
    }
}
