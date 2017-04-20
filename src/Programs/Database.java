package Programs;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import wrapper.Klout;
import wrapper.User;

public class Database {

    ResultSet tweets;
    Connection con;
    String kloutKey;

    public Database(String kloutKey) throws ClassNotFoundException, SQLException {

        Class.forName("com.mysql.jdbc.Driver");
        
        //insert port and password here
        con = DriverManager.getConnection("jdbc:mysql://"
                + "localhost:3306/project", "root", "password");

        PreparedStatement statement = con.prepareStatement("select * from twitter_data;");
        tweets = statement.executeQuery();
        this.kloutKey = kloutKey;
    }

    public void clenseTweets() throws SQLException {
                con.createStatement().execute("truncate twitter_data;");
    }

    public void clenseTriples() throws SQLException {
                con.createStatement().execute("truncate triples;");
    }

    public void clenseProbs() throws SQLException {
        con.createStatement().execute("truncate triple_probs;");
    }

    public void loadFile() throws SQLException {
        //WARNING: File should first be encoded in UTF-8
        con.createStatement().execute("load data infile "
                + "'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/cleanedData.csv' "
                + "ignore into table twitter_data fields terminated by '||' "
                + "ignore 1 lines;");
        PreparedStatement statement = con.prepareStatement("Select * from twitter_data");
        tweets = statement.executeQuery();
        statement.close();
    }

    public String getTweet() throws SQLException {
        if (tweets.next()) {
            return tweets.getString("msgBody");
        }
        return "";
    }

    public String getTweetID() throws SQLException {
        return tweets.getString("msgID");
    }

    //Returns true if a given tweet has already been processed
    //This is an optimization method for when the triple extraction software runs slowly
    public boolean uniqueTweet(String tweet) throws SQLException {
        PreparedStatement statement = con.prepareStatement("select min(msgID) "
                + "from twitter_data where msgBody=?");
        statement.setString(1, tweet);
        ResultSet rs1 = statement.executeQuery();
        rs1.next();

        if (rs1.getString(1) == null) {
            statement.close();
            return true;
        }

        //If the minimum msgID does not match the current msgID, then the
        //      current msgBody is not unique.  This means the triples from the
        //      earlier tweet can be reused for the current tweet.
        if (!rs1.getString(1).equals(tweets.getString("msgID"))) {
            statement = con.prepareStatement("select t1, t2, t3, extractProb "
                    + "from triples where msgID=?");
            statement.setString(1, rs1.getString(1));
            ResultSet rs2 = statement.executeQuery();
            while (rs2.next()) {
                insertTriple(rs2.getString(1), rs2.getString(2),
                        rs2.getString(3), rs2.getDouble("extractProb"));
            }
            statement.close();
            return false;
        } else {
            statement.close();
            return true;
        }
    }

    public void populateKlout() throws SQLException {
        //Use the klout key associated with your account here
        Klout k = new Klout(kloutKey);

        //Find all rows with null klout score by unique user
        PreparedStatement statement = con.prepareStatement("select d.username "
                + "from triples t, twitter_data d where t.msgId=d.msgID and "
                + "t.klout is null group by d.userID;");
        ResultSet rs1 = statement.executeQuery();
        int usernameNum = 1;
        while (rs1.next()) {
            System.out.println("fetching klout for user #" + usernameNum++);
            
            //Klout may not have a score for a given username
            //Initialize score as 0 and only update if a klout score is found
            double score = 0;
            try {
                String[] kloutID = k.getIdentity(rs1.getString("username"),
                        Klout.TWITTER_SCREEN_NAME);
                User u = k.getUser(kloutID[0]);
                score = u.score();
            } catch (Exception ex) {
                Logger.getLogger(Database.class.getName()).log(Level.SEVERE,
                        null, ex);
            }
            
            statement = con.prepareStatement("update triples "
                    + "set klout=?, kloutDateTime=? where msgID in "
                    + "(select msgID from twitter_data where username=?);");
            statement.setDouble(1, score);
            statement.setTimestamp(2,
                    java.sql.Timestamp.from(java.time.Instant.now()));
            statement.setString(3, rs1.getString("username"));
            statement.executeUpdate();
            statement.close();
        }
    }

    public void insertTriple(String t1, String t2, String t3, double conf)
            throws SQLException {
        PreparedStatement statement = con.prepareStatement("insert into "
                + "triples(t1, t2, t3, msgID, extractProb) "
                + "values (?, ?, ?, ?, ?);");
        statement.setString(1, t1);
        statement.setString(2, t2);
        statement.setString(3, t3);
        statement.setString(4, tweets.getString("msgID"));
        statement.setDouble(5, conf);
        statement.executeUpdate();

        //If the new triple is unique, assign a tripleID one greater than the 
        //      current greatest.  Otherwise the new triple is assigned the 
        //      same tripleID as the matching triples.
        statement = con.prepareStatement("select max(recordID) from triples");
        //The max record will be the current record
        ResultSet currentRecord = statement.executeQuery();
        currentRecord.next();

        //Check for a duplicate triple ignoring current record
        statement = con.prepareStatement("select tripleID from triples where "
                + "(t1, t2, t3) = (?, ?, ?) and recordID != ? limit 1;");
        statement.setString(1, t1);
        statement.setString(2, t2);
        statement.setString(3, t3);
        statement.setString(4, currentRecord.getString(1));
        ResultSet rs1 = statement.executeQuery();
        statement = con.prepareStatement("update triples set tripleID = ? "
                + "where recordID = ?");
        if (rs1.next()) {  //if there is a matching triple, reuse that tripleID
            statement.setString(1, rs1.getString("tripleID"));
        } else {  //else use the increment of the greatest existing tripleID
            PreparedStatement statement2 = con.prepareStatement("select "
                    + "ifnull(max(tripleID)+1,1) from triples");
            ResultSet rs2 = statement2.executeQuery();
            rs2.next();
            statement.setString(1, rs2.getString(1));
            statement2.close();
        }
        statement.setString(2, currentRecord.getString(1));
        statement.executeUpdate();
        statement.close();
    }

    //Returns all triples provided by all of the given sources
    public int[] getTriplesProvidedBySources(String[] usernames) throws SQLException {
        if(usernames.length<1){
            int[] noTriples = {};
            return noTriples;
        }
        
        //Initialize a result array containing the triples from the first username
        //While there are more usernames, keep the intersection between the result array and each new array
        
        PreparedStatement statement = con.prepareStatement("select t.tripleID from triples t, twitter_data d where t.msgId=d.msgID and d.username=?");
        statement.setString(1, usernames[0]);
        ResultSet rs1 = statement.executeQuery();
        List<Integer> resultList = new ArrayList<>();
        while (rs1.next()) {
            resultList.add(rs1.getInt(1));
        }
        
        for (int i = 1; i < usernames.length; i++) {
            statement.setString(1, usernames[i]);
            rs1 = statement.executeQuery();
            List<Integer> temp = new ArrayList<>();
            while (rs1.next()) {
                temp.add(rs1.getInt(1));
            }
            resultList.retainAll(temp);
        }

        int[] result = new int[resultList.size()];
        for(int i=0; i<result.length; i++){
            result[i] = resultList.get(i);
        }
        statement.close();
        return result;
    }

    public double[] getRecordProbabilities() throws SQLException {
        //calculate probability based on klout score and extraction probability
        PreparedStatement statement = con.prepareStatement("select klout, "
                + "extractProb from triples");
        ResultSet rs1 = statement.executeQuery();
        statement = con.prepareStatement("select count(*) from triples");
        ResultSet rs2 = statement.executeQuery();
        rs2.next();
        double[] result = new double[rs2.getInt(1)];
        int i = 0;
        while (rs1.next()) {
            //Result is klout score as a percentage times extraction probability
            result[i] = rs1.getDouble(1) / 100 * rs1.getDouble(2);
            i++;
        }
        statement.close();
        return result;
    }

    public String[] getConfirmingSources(int triple) throws SQLException {
        PreparedStatement statement = con.prepareStatement("select count(*) "
                + "from (select distinct(d.username) from triples t, "
                + "twitter_data d where t.msgID=d.msgID and t.tripleID=?)"
                + " as temp;");
        statement.setInt(1, triple);
        ResultSet rs1 = statement.executeQuery();
        statement = con.prepareStatement("select distinct(d.username) from "
                + "triples t, twitter_data d where t.msgID=d.msgID and "
                + "t.tripleID=?;");
        statement.setInt(1, triple);
        ResultSet rs2 = statement.executeQuery();
        rs1.next();
        String[] result = new String[rs1.getInt(1)];
        int i = 0;
        while (rs2.next()) {
            result[i++] = rs2.getString(1);
        }
        statement.close();
        return result;
    }

    public int[] getConfirmingRecords(int triple) throws SQLException {
        PreparedStatement statement = con.prepareStatement("select count(*) "
                + "from triples where tripleID=?;");
        statement.setInt(1, triple);
        ResultSet rs1 = statement.executeQuery();
        statement = con.prepareStatement("select recordID from triples "
                + "where tripleID=?;");
        statement.setInt(1, triple);
        ResultSet rs2 = statement.executeQuery();
        rs1.next();
        int[] result = new int[rs1.getInt(1)];
        int i = 0;
        while (rs2.next()) {
            result[i++] = rs2.getInt(1);
        }
        statement.close();
        return result;
    }
    
    public void insertAlpha(int triple, double alpha) throws SQLException{
        PreparedStatement statement = con.prepareStatement("insert into triple_probs(tripleID, alpha) values (?,?);");
        statement.setInt(1, triple);
        statement.setDouble(2, alpha);
        statement.executeUpdate();
        statement.close();
    }

    //This method left for future research
    public String[] getSourcesRejecting(int triple) {
        String[] result = {};
        return result;
    }

    public int getNumUniqueTriples() throws SQLException {
        PreparedStatement statement = con.prepareStatement("select "
                + "count(distinct(tripleID)) from triples");
        ResultSet rs1 = statement.executeQuery();
        rs1.next();
        int result = rs1.getInt(1);
        statement.close();
        return result;
    }

    public void insertProbability(int triple, double prob) throws SQLException {
        PreparedStatement statement = con.prepareStatement("update "
                + "triple_probs set prob=? where tripleID=?");
        statement.setDouble(1, prob);
        statement.setInt(2, triple);
        statement.executeUpdate();
        statement.close();
    }

    //Test method to find triple pairs where t1 and t2 are equal but t3 is different
    /*
    public void getDenyingTriples(int triple) throws SQLException {
        PreparedStatement statement = con.prepareStatement("select t1, t2, t3 "
                + "from triples where tripleID=?");
        statement.setInt(1, triple);
        ResultSet rs1 = statement.executeQuery();
        rs1.next();
        statement = con.prepareStatement("select tripleID, t1, t2, t3 from "
                + "triples where t1=? and t2=? and t3!=?;");
        statement.setString(1, rs1.getString(1));
        statement.setString(2, rs1.getString(2));
        statement.setString(3, rs1.getString(3));
        ResultSet rs2 = statement.executeQuery();
        while (rs2.next()) {
            System.out.println(rs2.getInt(1) + ": " + rs2.getString(2) + ", "
                    + rs2.getString(3) + ", " + rs2.getString(4));
        }
        statement.close();
    }*/
}
