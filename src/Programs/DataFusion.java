package Programs;

import java.io.IOException;
import java.sql.*;

public class DataFusion {

    public static void main(String[] args) throws SQLException,
            ClassNotFoundException,
            IOException {

        //Use these booleans to control what you want the program to do
        String fileNameForImport = "twitter_data.csv";
        boolean cleanFile = false;
        boolean loadFile = false;
        boolean clenseTweets = false;
        boolean clenseTriples = false;
        ExtractorType extractionType = ExtractorType.ReVerb;
        boolean importTriples = false;
        boolean populateKlout = false;
        boolean calculateProbabilities = true;
        CalculationType calcType = CalculationType.Exact;
        
        String kloutKey = "krukmswajmcyh2sqhspc55kz";
        double threshold = 0.6;

        Database db = new Database(kloutKey);
        
        Triples triples = new Triples(extractionType, db);
        
        if (cleanFile) {
            FileCleaner.cleanFile(fileNameForImport, 8514);
        }
        if (loadFile){
            db.loadFile();
        }
        if (clenseTweets) {
            db.clenseTweets();
        }
        if (clenseTriples) {
            db.clenseTriples();
        }
        if (importTriples) {
            triples.importTriples();
        }
        if (populateKlout) {
            db.populateKlout();
        }
        if (calculateProbabilities) {
            //db.clenseProbs();
            Probability prob = new Probability(db, triples, threshold);
            prob.calculateProbability(calcType);
        }

    }

}
