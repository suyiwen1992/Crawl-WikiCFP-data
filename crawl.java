import java.net.*;
import java.io.*;


public class crawl {
    public static int DELAY = 7;
    public static void main(String[] args) {
    
        try {
            
            String[] category=new String[4];
            category[0] = "data mining";
            category[1] = "databases";
            category[2] = "machine learning";
            category[3] = "artificial intelligence";
            
            int numOfPages = 20;
                        //create the output file
            File file = new File("wikicfp_crawl.txt");
            file.createNewFile();
            FileWriter writer = new FileWriter(file); 
            String result="";

        
            for(int k=0;k<4;k++){
             //now start crawling the all 'numOfPages' pages
              for(int i = 1;i<=numOfPages;i++) {
                //Create the initial request to read the first page 
                //and get the number of total results
                 String linkToScrape = "http://www.wikicfp.com/cfp/call?conference="+
                                  URLEncoder.encode(category[k], "UTF-8") +"&page=" + i;
                 String content = getPageFromUrl(linkToScrape);              
                //parse or store the content of page 'i' here in 'content'
                //YOUR CODE GOES HERE
                

                 String[] spilt_content=content.split("\n");// Spilt content

                 //Find the conference information. 
                 for(int j=0;j<spilt_content.length;j++){
 
                    if(spilt_content[j].indexOf("eventid")!=-1){
                        result+=spilt_content[j].replaceAll("<([^>]*)>","")+"\t";//conference_acronym
                        result+=spilt_content[j+1].replaceAll("<([^>]*)>","")+"\t";//conference_name
                        result+=spilt_content[j+5].replaceAll("<([^>]*)>","")+"\n";//conference_location
                        
                        j+=6;

                     }

                 }

                
                                
                //IMPORTANT! Do not change the following:
                Thread.sleep(DELAY*1000); //rate-limit the queries
            }
        }
                        writer.write(result);        
                
                System.out.println(result);

        writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
    
    /**
     * Given a string URL returns a string with the page contents
     * Adapted from example in 
     * http://docs.oracle.com/javase/tutorial/networking/urls/readingWriting.html
     * @param link
     * @return
     * @throws IOException
     */
    public static String getPageFromUrl(String link) throws IOException {
        URL thePage = new URL(link);
        URLConnection yc = thePage.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(
                                    yc.getInputStream()));
        String inputLine;
        String output = "";
        while ((inputLine = in.readLine()) != null) {
            output += inputLine + "\n";
        }
        in.close();
        return output;
    }
    
    
    
}
