package com.griffiths.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NegativeWordsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2706047697068872387L;
	
	private final int minWordLength;
	
	//List of generic negative words for the words in tweets to be checked against
    private Set<String> NEGATIVE_WORDS = new HashSet<String>(Arrays.asList(new String[] {
			"abysmal", "adverse", "alarming", "angry", "annoy", "anxious", "apathy", "appalling", "atrocious", "awful", "bad", 
			"banal", "barbed", "belligerent", "bemoan", "beneath", "boring", "broken", "callous", "can't", "clumsy", "coarse", 
			"cold", "cold-hearted", "collapse", "confused", "contradictory", "contrary", "corrosive", "corrupt", "crazy", "creepy",
			"criminal", "cruel", "cry", "cutting", "dead", "decaying", "damage", "damaging", "dastardly", "deplorable", "depressed", 
			"deprived", "deformed", "deny", "despicable", "detrimental", "dirty", "disease", "disgusting", "disheveled", "dishonest", 
			"dishonorable", "dismal", "distress", "don't", "dreadful", "dreary", "enraged", "eroding", "evil", "fail", "faulty", 
			"fear", "feeble", "fight", "filthy", "foul", "frighten", "frightful", "gawky", "ghastly", "grave", "greed", "grim", 
			"grimace", "gross", "grotesque", "gruesome", "guilty", "haggard", "hard", "hard-hearted", "harmful", "hate", "hideous", 
			"homely", "horrendous", "horrible", "hostile", "hurt", "hurtful", "icky", "ignore", "ignorant", "ill", "immature", 
			"imperfect", "impossible", "inane", "inelegant", "infernal", "injure", "injurious", "insane", "insidious", "insipid", 
			"jealous", "junky", "lose", "lousy", "lumpy", "malicious", "mean", "menacing", "messy", "misshapen", "missing", 
			"misunderstood", "moan", "moldy", "monstrous", "naive", "nasty", "naughty", "negate", "negative", "never", "no", 
			"nobody", "nondescript", "nonsense", "not", "noxious", "objectionable", "odious", "offensive", "old", "oppressive", 
			"pain", "perturb", "pessimistic", "petty", "plain", "poisonous", "poor", "prejudice", "questionable", "quirky", "quit", 
			"reject", "renege", "repellant", "reptilian", "repulsive", "repugnant", "revenge", "revolting", "rocky", "rotten", "rude", 
			"ruthless", "sad", "savage", "scare", "scary", "scream", "severe", "shoddy", "shocking", "sick", "sickening", "sinister", 
			"slimy", "smelly", "sobbing", "sorry", "spiteful", "sticky", "stinky", "stormy", "stressful", "stuck", "stupid", 
			"substandard", "suspect", "suspicious", "tense", "terrible", "terrifying", "threatening", "ugly", "undermine", "unfair", 
			"unfavorable", "unhappy", "unhealthy", "unjust", "unlucky", "unpleasant", "upset", "unsatisfactory", "unsightly", 
			"untoward", "unwanted", "unwelcome", "unwholesome", "unwieldy", "unwise", "upset", "vice", "vicious", "vile", "villainous", 
			"vindictive", "wary", "weary", "wicked", "woeful", "worthless", "wound", "yell", "yucky", "zero",
	}));
    private OutputCollector collector;

    public NegativeWordsBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }
    
    @Override
    public void execute(Tuple input) {
    	//int to count the number of negative words contained in tweets
    	int negWordCount = 0;
    	
    	//The positive word count is collected here so it can be sent to ScoreBolt
    	int posWordCount = (int) input.getValueByField("posWordCount");

        ArrayList<String> filteredWords = (ArrayList<String>) input.getValueByField("filteredWords");
        
        //For loop checks if the words are the required length 
    	//Also checks the against words in NEGATIVE_WORDS array
    	//If both conditions met, all negative words iterated to variable
        for (String word: filteredWords) {
            if ((word.length() >= minWordLength) & (NEGATIVE_WORDS.contains(word))) {
            	negWordCount++;
            }
        }
        //collector emits the count of positive words and count of negative words to next bolt
        collector.emit(new Values(posWordCount, negWordCount));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("posWordCount", "negWordCount"));
    }
}