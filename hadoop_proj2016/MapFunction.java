package hadoop_proj2016;

/**
 * Created by kostas on 6/9/16.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.lang.*;
import java.io.IOException;


public class MapFunction extends Mapper<LongWritable, Text, Text, Text> {
    private Text couple = new Text(); //output
    private Text friendsOfUser = new Text(); //output

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String[] allPeople = value.toString().split(" "); // Χωρίζεται με βάση το κενό και βάζει στη πρώτη θέση τον χρήστη και στη δεύτερη τους υπόλοιπους.
        String user = allPeople[0];
        if (allPeople.length > 1) {
            String userFriends = allPeople[1];
            String[] friends = userFriends.split(","); //χωρίζει τους φίλους με βάση το κόμμα και τους αποθηκεύει σε έναν πίνακα friends
            String[] arrayOfCouple = new String[2]; //σε αυτό το πίνακα θα αποθηκεύσει τον χρήστη και κάθε φορά έναν από τους φίλους του για να κάνει το ζευγάρι.

            for (int i = 0; i < friends.length; i++) {
                arrayOfCouple[0] = user;
                arrayOfCouple[1] = friends[i];
                System.out.println("##############################" + "user:" + arrayOfCouple[0] + " " + "friend:" + arrayOfCouple[1] + "#####################");
                //Ταξινομούμε τον πίνακα με τα μέλη του ζεύγους έτσι ώστε με όποια σειρά και να εμφανιστούν το τελικό ΚΕΥ να είναι το ίδιο..πχ όταν ο χρήστης είναι ο τζον
                //το ζεύγος θα εμφανιστεί ως (John,Alice) όμως το αλλάζουμε σε (Alice,John)..βοηθάει στην έξοδο της map.
                int j;
                boolean flag = true;  // καθορίζει αν η ταξινόμηση τελείωσε.
                String temp;

                while (flag) {
                    flag = false;
                    for (j = 0; j < arrayOfCouple.length - 1; j++) {
                        if (arrayOfCouple[j].compareToIgnoreCase(arrayOfCouple[j + 1]) > 0) {             // ascending sort
                            temp = arrayOfCouple[j];
                            arrayOfCouple[j] = arrayOfCouple[j + 1];     // ενναλαγή
                            arrayOfCouple[j + 1] = temp;
                            flag = true;
                        }
                    }
                }

                String theCouple = arrayOfCouple[0].concat("-").concat(arrayOfCouple[1]); //τα ενώνει σε ένα String και δημιουργεί το ζευγάρι με -
                System.out.println("##############################" + "COUPLE:" + theCouple + "#####################");
                couple.set(theCouple); // px Alice-John

                String someFriends = "";
                for (int l = 0; l < friends.length; l++) {

                    someFriends = someFriends.concat(friends[l]).concat(" "); //ενώνει όλους τους φίλους του χρήστη σε ένα string

                }
                System.out.println("##############################" + "COUPLE's FRIENDS:" + someFriends + "#####################");
                friendsOfUser.set(someFriends);


                context.write(couple, friendsOfUser); //έξοδος

                // είτε διαβάσει (Alice,John), αν διαβάζει τη σειρά όπου ο χρήστης είναι η Alice, είτε (John,Alice), αν διαβάζει τη σειρά όπου ο χρήστης είναι ο τζον,..
                // μετατρέπεται σε (Alice-John) συμφωνα με το ποιο είναι πρώτο αλφαβητικά και
                // εφόσον καθώς διαβάζει σειρές θα εμφανιστούν δύο (Alice-John) to shuffle το μετατραπεί σε
                // (Alice-John,[friendsOfUser,friendsOfUser]) και το στέλνει στην reduce δηλαδή θα έχουμε και τους φίλους της Alice και τους φίλους του John.
            }


        }//end of if split

    }

}

