/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Programs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Scanner;

/**
 *
 * @author Richard
 */
public class FileCleaner {

    public static void cleanFile(String fileName, int entries) 
            throws FileNotFoundException, SQLException {
        Scanner scan = new Scanner(new File(fileName));
        PrintWriter write = new PrintWriter("C:\\ProgramData\\MySQL\\"
                + "MySQL Server 5.7\\Uploads\\cleanedData.csv");
        
        //File is a .csv with || as the delimiter
        scan.useDelimiter("\\|\\|");
        String curr;  //holds current element being manipulated
        for (int i = 0; i <= entries; i++) {
            System.out.print(i + ": ");
            //Skip over first three columns (these do not need maniuplation)
            for (int j = 0; j < 3; j++) {
                write.print(scan.next() + "||");
            }
            curr = scan.next();
            //curr is now poitioned on messageBody.
            
            //Remove any urls from message body
            curr += " ";
            curr = curr.replaceAll("htt.*?\\s", " ");
            
            //Remove all nonstandard symbols and line breaks from message body
            curr = curr.replaceAll("[^A-Za-z0-9',.;():\"\\-\\?!/ ]", "");
            
            curr = curr.trim();
            
            write.print(curr + "||");
            
            System.out.println(curr);
            
            //Skip over remaining columns in current row
            for (int j = 0; j < 13; j++) {
                write.print(scan.next() + "||");
            }
        }
        write.print(scan.next());
        scan.close();
        write.flush();
        write.close();
    }
}
