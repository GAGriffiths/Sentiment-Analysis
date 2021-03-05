package com.griffiths.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class ScoreBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 2706047697068872387L;

    private static final Logger logger = LoggerFactory.getLogger(ScoreBolt.class);

    /** Number of seconds before the list will be logged to stdout. */
    private final long logIntervalSec;

    private long lastLogTime;
    
    private int tweetsAnalysed;
        
    private double posTweets, negTweets;

    public ScoreBolt(long logIntervalSec) {
        this.logIntervalSec = logIntervalSec;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        lastLogTime = System.currentTimeMillis();
    }
    
    //this is the final bolt so this function is empty 
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void execute(Tuple input) {
    	//This is getting the positive and negative word count
    	//and converting them to ints to be manipulated in counts
        int posWords = (int) input.getValueByField("posWordCount");
        int negWords = (int) input.getValueByField("negWordCount");
        
        //the positive and negative tweet score is calculated using the
        //positive word and negative word count
        int tweetScore = posWords - negWords;
        
        //the tweet score is positive if greater than 0
        //anything that is 0 is less than that is negative
        if (tweetScore > 0) {
            posTweets++;
        } else {
            negTweets++;
        }
        
        tweetsAnalysed++;
        
        publishTweetList();
    }
    
    private void publishTweetList() {
    	//Set up for logger output
    	long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {
        	//creating the positive and negative score
        	double posTweetScore = posTweets / tweetsAnalysed * 100;
            double negTweetScore = negTweets / tweetsAnalysed * 100;
            
            //final output, with String.format formatting doubles
            //to make them more presentable            
            logger.info("\n");
            logger.info("COVID-19 Related Tweets Analysed: " + tweetsAnalysed);
            logger.info("Positive COVID-19 Tweets: " + String.format("%.0f", posTweets));
            logger.info("Negative COVID-19 Tweets: " + String.format("%.0f", negTweets));
            logger.info("Positive COVID-19 Tweet Score: " + String.format("%.2f", posTweetScore) + "%");
            logger.info("Negative COVID-19 Tweet Score: " + String.format("%.2f", negTweetScore) + "%");
            
            //ensures that the logger output is output, but also only output every 60 seconds
            lastLogTime = now;
        }
    }
}