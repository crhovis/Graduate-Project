package Programs;

import java.sql.SQLException;

/**
 *
 * @author Richard
 */
public class Statistics {

    private String[] input;
    private final double[] alphas;
    private final int numTriples;
    private final double threshold;
    private int totalTrue = 0;
    private final Database db;

    public Statistics(Database db, double threshold, int numTriples, double[] alphas) throws SQLException {
        this.threshold = threshold;
        this.alphas = alphas;
        for (int i = 0; i < alphas.length; i++) {
            if (alphas[i] >= threshold) {
                totalTrue++;
            }
        }
        this.numTriples = numTriples;
        this.db = db;
    }

    //input is an array of sources on which to calculate joint recall and joint fpr
    public void setInput(String[] input) {
        this.input = input;
    }

    //This method calculates joint recall and joint false positive rate
    //      simultaneously, reducing the number of database calls
    public double[] getStats() throws SQLException {

        int jointRecallNumerator = 0, jointFprNumerator = 0;

        int[] triples = db.getTriplesProvidedBySources(input);
        
        //If triple is thought to be true, increase recall
        //Else increase false positive rate
        for (int i = 0; i < triples.length; i++) {
            if (alphas[triples[i] - 1] >= threshold) {
                jointRecallNumerator++;
            } else {
                jointFprNumerator++;
            }
        }
        double[] result = new double[2];
        result[0] = (double) jointRecallNumerator / totalTrue;
        result[1] = (double) jointFprNumerator / (numTriples - totalTrue);
        return result;
    }

}
