package com.griffiths.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IgnoreWordsBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 6069146554651714100L;
	
	private OutputCollector collector;
	
	//This is a list of irrelevant words that are filtered out, not to be counted
	private Set<String> IGNORE_WORDS = new HashSet<String>(Arrays.asList(new String[] {
            "http", "https", "the", "you", "que", "and", "for", "that", "like", "have", "this", 
            "just", "with", "all", "get", "about", "can", "was", "not", "your", "but", "are", "one",
            "what", "out", "when", "get", "lol", "now", "para", "por", "want", "will", "know",
            "good", "from", "las", "don", "people", "got", "why", "con", "time", "would",
    }));
	
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }
    
    @Override
    public void execute(Tuple input) {
    	//This gets the tweet and removes any new lines or punctuation found in the tweet
        Status tweet = (Status) input.getValueByField("tweet");
        String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        
        //Arrays for collecting words
        ArrayList<String> filteredWords = new ArrayList<String>();
        ArrayList<String> words = new ArrayList<String> (Arrays.asList(text.split(" ")));
        
        //For loop checks words that are not in IGNORE_WORDS and adds to filteredWords array
        for (int i = 0; i < words.size(); i++) {
            if (!IGNORE_WORDS.contains(words.get(i))) {
                filteredWords.add(words.get(i));
            }
        }
        //the collector emits the filtered tweets to next bolt
        collector.emit(new Values(filteredWords));
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("words"));
    }
}