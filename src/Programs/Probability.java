package Programs;

import java.sql.SQLException;

public class Probability {

    double alphas[];
    int numTriples;
    double threshold;
    Database db;
    Triples triples;
    Statistics stat;

    public Probability(Database db, Triples triples, double threshold) throws SQLException {
        this.db = db;
        this.threshold = threshold;
        this.numTriples = db.getNumUniqueTriples();
        this.triples = triples;
        this.alphas = triples.calculateCombinedAlphas(numTriples);
        stat = new Statistics(db, threshold, numTriples, alphas);
    }

    public void calculateProbability(CalculationType calcType) throws SQLException {
        if (calcType.equals(CalculationType.Independent)) {
            calculateIndependentProbability();
        }
        if (calcType.equals(CalculationType.Exact)) {
            calculateExactProbability();
        }
        if (calcType.equals(CalculationType.Approximate)) {
            calculateApproximateProbability();
        }
    }

    //Based on formula (8) from paper:
    //Pochampally, Ravali et al. Fusing Data with Correlations. ACM.
    private void calculateIndependentProbability() throws SQLException {

        for (int i = 1; i <= numTriples; i++) {
            System.out.print("probability of triple " + i + " out of " + numTriples + ": ");
            String[] sourcesConfirming = db.getConfirmingSources(i);
            
            //For each confirming source, keep a running product of the recall of that source over the fpr of that source
            double muProduct = 1;
            for (int j = 0; j < sourcesConfirming.length; j++) {
                String[] input = {sourcesConfirming[j]};
                stat.setInput(input);
                double[] stats = stat.getStats();
                muProduct *= stats[0] / stats[1];
            }
            double prob = 1 / (1 + (1 - alphas[i - 1]) / alphas[i - 1] * 1 / muProduct);
            System.out.println(prob);
            db.insertProbability(i, prob);
        }

    }

    //Based on formulas (10), (11), and (12) from paper:
    //Pochampally, Ravali et al. Fusing Data with Correlations. ACM.
    private void calculateExactProbability() throws SQLException {
        Combinations comb = new Combinations();

        for (int i = 1; i <= numTriples; i++) {  //i indicates the current triple
            System.out.print("probability of triple " + i + " out of " + numTriples + ": ");
            String[] sourcesConfirming = db.getConfirmingSources(i);
            String[] sourcesRejecting = db.getSourcesRejecting(i);
            comb.setInput(sourcesConfirming, sourcesRejecting);
            double muNumerator = 0;
            double muDenominator = 0;
            String[] currentCombination;
            while (comb.hasNext()) {
                currentCombination = comb.next();
                stat.setInput(currentCombination);
                double[] stats = stat.getStats();

                muNumerator += Math.pow(-1, currentCombination.length - sourcesConfirming.length) * stats[0];
                muDenominator += Math.pow(-1, currentCombination.length - sourcesConfirming.length) * stats[1];
            }
            double mu = muNumerator / muDenominator;
            double prob = 1 / (1 + (1 - alphas[i - 1]) / alphas[i - 1] * 1 / mu);
            System.out.println(prob);
            db.insertProbability(i, prob);
        }
    }

    //Method was never implemented because the exact calculations ran in a resaonable amount of time
    private void calculateApproximateProbability() {

    }
}
