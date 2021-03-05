package com.griffiths.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {
	
    static final String TOPOLOGY_NAME = "BigDataAssignmentTwo";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
        
        //Creating the topology
        TopologyBuilder b = new TopologyBuilder();
        
        //Spout to take in tweets from Twitter Stream
        b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        
        //Bolt to ignore words using TwitterSampleSpout
        b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).shuffleGrouping("TwitterSampleSpout");
        
        //Bolt to distinguish whether a tweet is positive using IgnoreWordsBolt
        b.setBolt("PositiveWordsBolt", new PositiveWordsBolt(5)).shuffleGrouping("IgnoreWordsBolt");
        
        //Bolt to distinguish whether a tweet is negative using PositiveWordsBolt
        b.setBolt("NegativeWordsBolt", new NegativeWordsBolt(5)).shuffleGrouping("PositiveWordsBolt");
        
        //Bolt to determine positive/negative using NegativeWordsBolt
        b.setBolt("ScoreBolt", new ScoreBolt(60)).shuffleGrouping("NegativeWordsBolt");       
        
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });
    }
}