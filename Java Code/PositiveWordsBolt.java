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

public class PositiveWordsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2706047697068872387L;
	
	private final int minWordLength;
	
	private OutputCollector collector;
	
	//List of generic positive words for the words in tweets to be checked against
    private Set<String> POSITIVE_WORDS = new HashSet<String>(Arrays.asList(new String[] {
			"absolutely", "adorable", "accepted", "acclaimed", "accomplish", "accomplishment", "achievement", "action", "active", 
			"admire", "adventure", "affirmative", "affluent", "agree", "agreeable", "amazing", "angelic", "appealing", "approve", 
			"aptitude", "attractive", "awesome", "beaming", "beautiful", "believe", "beneficial", "bliss", "bountiful", "bounty", 
			"brave", "bravo", "brilliant", "bubbly", "calm", "celebrated", "certain", "champ", "champion", "charming", "cheery", 
			"choice", "classic", "classical", "clean", "commend", "composed", "congratulation", "constant", "cool", "courageous", 
			"creative", "cute", "dazzling", "delight", "delightful", "distinguished", "divine", "earnest", "easy", "ecstatic", 
			"effective", "effervescent", "efficient", "effortless", "electrifying", "elegant", "enchanting", "encouraging", "endorsed", 
			"energetic", "energized", "engaging", "enthusiastic", "essential", "esteemed", "ethical", "excellent", "exciting", 
			"exquisite", "fabulous", "fair", "familiar", "famous", "fantastic", "favorable", "fetching", "fine", "fitting", 
			"flourishing", "fortunate", "free", "fresh", "friendly", "fun", "funny", "generous", "genius", "genuine", "giving", 
			"glamorous", "glowing", "good", "gorgeous", "graceful", "great", "green", "grin", "growing", "handsome", "happy", 
			"harmonious", "healing", "healthy", "hearty", "heavenly", "honest", "honorable", "honored", "hug", "idea", "ideal", 
			"imaginative", "imagine", "impressive", "independent", "innovate", "innovative", "instant", "instantaneous", "instinctive", 
			"intuitive", "intellectual", "intelligent", "inventive", "jovial", "joy", "jubilant", "keen", "kind", "knowing", 
			"knowledgeable", "laugh", "legendary", "light", "learned", "lively", "lovely", "lucid", "lucky", "luminous", "marvelous", 
			"masterful", "meaningful", "merit", "meritorious", "miraculous", "motivating", "moving", "natural", "nice", "novel", 
			"now", "nurturing", "nutritious", "okay", "one", "one-hundred percent", "open", "optimistic", "paradise", "perfect", 
			"phenomenal", "pleasurable", "plentiful", "pleasant", "poised", "polished", "popular", "positive", "powerful", 
			"prepared", "pretty", "principled", "productive", "progress", "prominent", "protected", "proud", "quality", "quick", 
			"quiet", "ready", "reassuring", "refined", "refreshing", "rejoice", "reliable", "remarkable", "resounding", "respected", 
			"restored", "reward", "rewarding", "right", "robust", "safe", "satisfactory", "secure", "seemly", "simple", "skilled", 
			"skillful", "smile", "soulful", "sparkling", "special", "spirited", "spiritual", "stirring", "stupendous", "stunning", 
			"success", "successful", "sunny", "super", "superb", "supporting", "surprising", "terrific", "thorough", "thrilling", 
			"thriving", "tops", "tranquil", "transforming", "transformative", "trusting", "truthful", "unreal", "unwavering", "up", 
			"upbeat", "upright", "upstanding", "valued", "vibrant", "victorious", "victory", "vigorous", "virtuous", "vital", 
			"vivacious", "wealthy", "welcome", "well", "whole", "wholesome", "willing", "wonderful", "wondrous", "worthy", 
			"wow", "yes", "yummy", "zeal", "zealous",
    }));
    
    public PositiveWordsBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
    	//int to count the number of positive words contained in tweets
    	int posWordCount = 0;

    	ArrayList<String> filteredWords = (ArrayList<String>) input.getValueByField("words");  
    	
    	//For loop checks if the words are the required length 
    	//Also checks the against words in POSITIVE_WORDS array
    	//If both conditions met, all positive words iterated to variable
        for (String word: filteredWords) {
            if ((word.length() >= minWordLength) & (POSITIVE_WORDS.contains(word))) {
                posWordCount++;
            }
        }
        //collector emits the count of positive words, and the filtered tweet to next bolt
        collector.emit(new Values(filteredWords, posWordCount));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("filteredWords", "posWordCount"));
    }
}