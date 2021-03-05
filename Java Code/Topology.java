package com.griffiths.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {
	
    static final String TOPOLOGY_NAME = "COVIDSentimentAnalysis";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
        
        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("PositiveWordsBolt", new PositiveWordsBolt(5)).shuffleGrouping("IgnoreWordsBolt");
        b.setBolt("NegativeWordsBolt", new NegativeWordsBolt(5)).shuffleGrouping("PositiveWordsBolt");
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