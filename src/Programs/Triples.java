package Programs;

/* For representing a sentence that is annotated with pos tags and np chunks.*/
import edu.stanford.nlp.ie.util.RelationTriple;
import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import edu.washington.cs.knowitall.nlp.ChunkedSentence;

/* String -> ChunkedSentence */
import edu.washington.cs.knowitall.nlp.OpenNlpSentenceChunker;

/* The class that is responsible for extraction. */
import edu.washington.cs.knowitall.extractor.ReVerbExtractor;

/* The class that is responsible for assigning a confidence score to an
 * extraction.
 */
import edu.washington.cs.knowitall.extractor.conf.ConfidenceFunction;
import edu.washington.cs.knowitall.extractor.conf.ReVerbOpenNlpConfFunction;

/* A class for holding a (arg1, rel, arg2) triple. */
import edu.washington.cs.knowitall.nlp.extraction.ChunkedBinaryExtraction;
import java.io.IOException;

import java.sql.SQLException;

public class Triples {

    ExtractorType extractionType;
    Database db;

    public Triples(ExtractorType extractionType, Database db) {
        this.extractionType = extractionType;
        this.db = db;
    }

    public void importTriples() throws SQLException, IOException {
        //This threshold used to throw away any low confidence triples extracted
        double threshold = 0.7;
        int tweetNum = 1;
        String tweet = db.getTweet();
        while (!tweet.equals("")) {
            System.out.println("extracting triples from tweet " + tweetNum++);
            if (db.uniqueTweet(tweet)) {
                if (extractionType.equals(ExtractorType.ReVerb)) {
                    importTriplesReVerb(tweet, threshold);
                }
                if (extractionType.equals(ExtractorType.OpenIE)) {
                    importTriplesOpenIE(tweet, threshold);
                }
            }
            tweet = db.getTweet();
        }
    }

    private void importTriplesReVerb(String tweet, double threshold) throws IOException, SQLException {
        OpenNlpSentenceChunker chunker = new OpenNlpSentenceChunker();
        ChunkedSentence sent = chunker.chunkSentence(tweet);

        ReVerbExtractor reverb = new ReVerbExtractor();
        ConfidenceFunction confFunc = new ReVerbOpenNlpConfFunction();
        for (ChunkedBinaryExtraction extr : reverb.extract(sent)) {
            double conf = confFunc.getConf(extr);
            //only insert triples above threshold confidence
            if (conf >= threshold) {
                db.insertTriple(extr.getArgument1().getText(),
                        extr.getRelation().getText(),
                        extr.getArgument2().getText(), conf);
            }
        }
    }

    private void importTriplesOpenIE(String tweet, double threshold) throws SQLException {

        Document doc = new Document(tweet);
        // Iterate over the sentences in the document
        for (Sentence sent : doc.sentences()) {
            // Iterate over the triples in the sentence
            for (RelationTriple triple : sent.openieTriples()) {
                if (triple.confidence >= threshold) {
                    db.insertTriple(triple.subjectLemmaGloss(), 
                            triple.relationLemmaGloss(), 
                            triple.objectLemmaGloss(), triple.confidence);
                }
            }
        }
    }

    //Each triple can occur more than once with a probability associated with each instance.
    //These probabilities need to be combined into a single alpha value per unique triple.
    public double[] calculateCombinedAlphas(int numTriples)
            throws SQLException {
        double[] recordProbabilities = db.getRecordProbabilities();
        double[] alphasCombined = new double[numTriples];
        for (int triple = 1; triple <= numTriples; triple++) {
            int[] confirmingRecords = db.getConfirmingRecords(triple);

            //combinedProb = 1 - (1-p_1)*(1-p_2)*...
            double probMultiplication = 1;
            for (int j = 0; j < confirmingRecords.length; j++) {
                probMultiplication *= (1-recordProbabilities[confirmingRecords[j] - 1]);
            }
            alphasCombined[triple - 1] = (double) 1 - probMultiplication;
            //db.insertAlpha(triple, alphasCombined[triple-1]);
            System.out.println("combined probability of triple " + triple + ": " + alphasCombined[triple - 1]);
        }
        return alphasCombined;
    }

}
