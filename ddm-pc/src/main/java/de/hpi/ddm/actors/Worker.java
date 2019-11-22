package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Worker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Props props() {
        return Props.create(Worker.class);
    }

    public Worker() {
        this.cluster = Cluster.get(this.context().system());
        this.runner = null;

        this.log().info("Spawned new worker");
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkMessage implements Serializable {
        private static final long serialVersionUID = 6303081601689723395L;
        char[] permutationSet;
        char[] prefix;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintMessage implements Serializable {
        private static final long serialVersionUID = -6303081601689723391L;
        Map<String, List<Integer>> usedHashes;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CrackMessage implements Serializable {
        private static final long serialVersionUID = -1303081601689723393L;
        int row;
        String pwHash;
        String[] hints;
        char[] allPossibleCharacters;
        int passwordLength;
    }

    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;

    private Map<String, List<Integer>> hashedHints;
    private Thread runner;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);

        this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(WorkMessage.class, this::handle)
                .match(HintMessage.class, this::handle)
                .match(CrackMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(CurrentClusterState message) {
        message.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(MemberUp message) {
        this.register(message.member());
    }

    private void register(Member member) {
        if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
            this.masterSystem = member;

            this.getContext().actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME).tell(new Master.RegistrationMessage(), this.self());
        }
    }

    private void handle(MemberRemoved message) {
        if (this.masterSystem.equals(message.member()))
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void handle(WorkMessage message) {
        this.log().info("Got new work " + new String(message.getPermutationSet()));
        ActorRef master = this.sender();
        if (this.runner != null) this.runner.stop();    // stop() is deprecated. I think this sometime causes the error 'JDWP exit error AGENT_ERROR_INVALID_EVENT_TYPE(204): ExceptionOccurred [eventHelper.c:834]'
        this.runner = new Thread(() -> {
            List<String> allPermutations = new ArrayList<>();
            this.heapPermutation(message.getPermutationSet(), message.getPermutationSet().length, allPermutations);

            String prefix = new String(message.getPrefix());

            for (String permutation : allPermutations) {
                String hash = hash(prefix + permutation);
                List<Integer> rows = this.hashedHints.get(hash);
                if (rows != null) {
                    // This is okay because the array only contains a few rowIds
                    int[] rowArray = new int[rows.size()];
                    for (int i = 0; i < rowArray.length; i++) rowArray[i] = rows.get(i);
                    master.tell(new Master.SolvedHintMessage(prefix + permutation, hash, rowArray), this.self());
                }
            }

            master.tell(new Master.ReadyMessage(), this.self());
        });
        this.runner.start();
    }

    private void handle(HintMessage message) {
        this.hashedHints = message.getUsedHashes();
    }

    private void handle(CrackMessage message) {
        this.log().info("Got new crack request");
        ActorRef master = this.sender();
        Character[] usedChars = this.getUsedChars(message.getAllPossibleCharacters(), message.getHints());

        List<String> allCombinations = new ArrayList<>();
        generateCombinations(usedChars, "", usedChars.length, message.getPasswordLength(), allCombinations);
        for (String oneCombination : allCombinations) {
            if (hash(oneCombination).equals(message.getPwHash())) {
                master.tell(new Master.SolvedCrackMessage(message.getRow(), oneCombination), this.self());
                return;
            }
        }
    }

    private String hash(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));

            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < hashedBytes.length; i++) {
                stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    // Generating all permutations of an array using Heap's Algorithm
    // https://en.wikipedia.org/wiki/Heap's_algorithm
    // https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
    private void heapPermutation(char[] a, int size, List<String> l) {
        // If size is 1, store the obtained permutation
        if (size == 1)
            l.add(new String(a));

        for (int i = 0; i < size; i++) {
            heapPermutation(a, size - 1, l);

            // If size is odd, swap first and last element
            if (size % 2 == 1) {
                char temp = a[0];
                a[0] = a[size - 1];
                a[size - 1] = temp;
            }

            // If size is even, swap i-th and last element
            else {
                char temp = a[i];
                a[i] = a[size - 1];
                a[size - 1] = temp;
            }
        }
    }

    private void generateCombinations(Character[] set, String prefix, int n, int k, List<String> l) {

        // Base case: k is 0,
        // print prefix
        if (k == 0) {
            l.add(prefix);
            return;
        }

        // One by one add all characters
        // from set and recursively
        // call for k equals to k-1
        for (int i = 0; i < n; ++i) {

            // Next character of input added
            String newPrefix = prefix + set[i];

            // k is decreased, because
            // we have added a new character
            generateCombinations(set, newPrefix, n, k - 1, l);
        }
    }

    private Character[] getUsedChars(char[] characterUniverse, String[] hints) {
        Set<Character> inPw = new HashSet<>();
        List<Character> notInPw = new ArrayList<>();

        for (String hint : hints) {
            for (char c : characterUniverse) inPw.add(c);
            for (char c : hint.toCharArray()) inPw.remove(c);

            notInPw.addAll(inPw);
            inPw.clear();
        }

        List<Character> usedChars = new ArrayList<>();
        for(char c : characterUniverse) usedChars.add(c);
        usedChars.removeAll(notInPw);

        return usedChars.toArray(new Character[0]);
    }
}