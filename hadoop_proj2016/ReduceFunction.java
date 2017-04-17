package hadoop_proj2016;

/**
 * Created by kostas on 6/9/16.
 */
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.lang.*;
import java.io.IOException;

public class ReduceFunction<KEY> extends Reducer<KEY, Text, Text, Text> {
    private Text suggestedFriend = new Text();
    private Text theUser = new Text();

    public void reduce(KEY key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("#################################" + "REDUCE FUNCTION" + "##############################");
        String[] array_couple = key.toString().split("-"); //χωρίζει τους δύο χρηστες
        String userOne = array_couple[0]; //βάζει τον πρώτο χρήστη στην πρώτη θέση του πίνακα
        System.out.println("##############################" + "USER1:" + userOne + "#####################");
        if (array_couple.length > 1) {
            String userTwo = array_couple[1]; //βάζει τον δεύτερο χρήστη στην δεύτερη θέση του πίνακα
            System.out.println("##############################" + "USER2:" + userTwo + "#####################");
            String[] friendsOne = new String[0];
            String[] friendsTwo = new String[0];
            for (Text val : values) {
                String friends = val.toString();
                if (friends.contains(userOne)) {
                    friendsTwo = friends.split(" "); // Αν περιέχει τον πρώτο χρήστη στους φίλους τότε πρόκειται για τους φίλους του δεύτερου χρήστη.
                }
                if (friends.contains(userTwo)) {
                    friendsOne = friends.split(" "); // Αν περιέχει στους φίλους τον δεύτερο χρήστη πρόκειται για τους φίλους του πρώτου χρήστη.Καθώς δεν μπορεί κάποιος να είναι φίλος με τον εαυτό του.
                }

            } // τώρα οι πίνακες friendsOne, friendsTwo είναι γεμάτοι.

            for (int g = 0; g < friendsOne.length; g++) {
                System.out.println("Friend of User1:" + friendsOne[g]); //delete
            }
            for (int g = 0; g < friendsTwo.length; g++) {
                System.out.println("Friend of User2:" + friendsTwo[g]); //delete
            }


            //Τώρα ελέγχω για κοινούς ανάμεσα στους φίλους του 1ου και του 2ου χρήστη και βρίσκω τον αριθμό των κοινών.
            int common = 0;

            for (int i = 0; i < friendsOne.length; i++) {
                for (int j = 0; j < friendsTwo.length; j++) {
                    if (friendsOne[i].equalsIgnoreCase(friendsTwo[j])) {
                        common++;
                    }

                }

            }
            System.out.println("NUMBER OF COMMON FRIENDS OF USER 1 AND USER 2:" + common); //delete

            //Αποθηκεύουμε όλους τους κοινούς φίλους σε έναν πίνακα commonFriends .
            String[] commonFriends = new String[common];
            int c = 0;

            for (int i = 0; i < friendsOne.length; i++) {
                for (int j = 0; j < friendsTwo.length; j++) {
                    if (friendsOne[i].equalsIgnoreCase(friendsTwo[j])) {
                        commonFriends[c] = friendsOne[i];
                        c++;
                    }

                }

            }


            for (int i = 0; i < commonFriends.length; i++) {
                System.out.println("COMMON FRIEND OF USER 1 AND USER 2:" + commonFriends[i]); //delete
            }

// Suggest Friends

            if (common >= 5) {

                //Suggest a friend for userOne
                //Διασχίζω τον πίνακα με τους φίλους του δεύτερου χρήστη και αν δεν ανήκουν στους κοινούς τους προτείνω.
                for (int h = 0; h < friendsTwo.length; h++) {
                    boolean found = false; // αληθές αν βρέθηκε στους κοινούς
                    int cm = 0;
                    while (cm < commonFriends.length && found == false) {
                        if (friendsTwo[h].equalsIgnoreCase(commonFriends[cm])) {
                            found = true;


                        } else {
                            cm++;
                        }
                    }

                    if ((found == false) && !(friendsTwo[h].equalsIgnoreCase(userOne))) {
                        System.out.println("SUGGESTED FRIEND FOR :" + userOne + " " + "is" + " " + friendsTwo[h]);

                        theUser.set(userOne);
                        suggestedFriend.set(friendsTwo[h]);
                        context.write(theUser, suggestedFriend); //έξοδος
                    }
                }

                //επαναλαμβάνω τη διαδικασία και για το δεύτερο χρήστη.

                for (int h = 0; h < friendsOne.length; h++) {
                    boolean found = false;
                    int cm = 0;
                    while (cm < commonFriends.length && found == false) {
                        if (friendsOne[h].equalsIgnoreCase(commonFriends[cm])) {
                            found = true;


                        } else {
                            cm++;
                        }
                    }

                    if ((found == false) && !(friendsOne[h].equalsIgnoreCase(userTwo))) {
                        System.out.println("SUGGESTED FRIEND FOR :" + userTwo + " " + "is" + " " + friendsOne[h]);

                        theUser.set(userTwo);
                        suggestedFriend.set(friendsOne[h]);
                        context.write(theUser, suggestedFriend);
                    }
                }


            }


        } //end of if split

    }

}

