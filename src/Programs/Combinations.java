package Programs;

import java.util.ArrayList;
import java.util.List;

public class Combinations {

    //This class provides combinations required for formulas (10) and (11) from paper:
    //Pochampally, Ravali et al. Fusing Data with Correlations. ACM.
    
    //Each combination is the union of sources confirming a triple with S* (contained within sources rejecting a triple)
    
    private final List<String[]> result = new ArrayList<>();

    public void setInput(String[] sourcesConfirming, String[] sourcesRejecting) {
        //First result is for empty string in S*
        result.add(sourcesConfirming);
        
        //Subsequent results require calls to combinationRecursion
        for (int i = 1; i <= sourcesRejecting.length; i++) {
            String[] data = new String[i];
            combinationRecursion(sourcesConfirming, sourcesRejecting, data, 0, sourcesRejecting.length - 1, 0, i);
        }
    }

    //Method is a rework of one provided by GeeksForGeeks
    //http://www.geeksforgeeks.org/
    //find-all-possible-combinations-of-r-elements-in-a-given-array-of-size-n/
    private void combinationRecursion(String[] sourcesConfirming, String[] sourcesRejecting, String[] data,
            int start, int end, int index, int r) {
        if (index == r) {
            String[] result = new String[sourcesConfirming.length + data.length];
            int j;
            for (j = 0; j < sourcesConfirming.length; j++) {
                result[j] = sourcesConfirming[j];
            }
            for (int k = 0; k < data.length; k++) {
                result[j++] = data[k];
            }
            this.result.add(result);
            return;
        }
        for (int i = start; i <= end && end - i + 1 >= r - index; i++) {
            data[index] = sourcesRejecting[i];
            combinationRecursion(sourcesConfirming, sourcesRejecting, data, i + 1, end, index + 1, r);
        }
    }

    public boolean hasNext() {
        if (result.isEmpty()) {
            return false;
        }
        return true;
    }

    public String[] next() {
        return result.remove(0);
    }

}
