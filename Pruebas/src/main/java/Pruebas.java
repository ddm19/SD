import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.ResponseBody;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

public class Pruebas
{
    private static String LeeArchivo(String archivo,int dato)
    {
        String clave = "";
        try
        {
            File myObj = new File(archivo);
            Scanner myReader = new Scanner(myObj);
            int i = 0;
            while (myReader.hasNextLine() && i < dato)
            {
                String data = myReader.nextLine();
                clave = data;
                i++;
            }
            myReader.close();
        }
        catch (FileNotFoundException e)
        {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        return clave;
    }
    public static void main(String[] args)
    {
        float zonas[] = new float[4];

    }
    public static JSONObject http(String ciudad,String apikey) throws IOException, ParserConfigurationException, SAXException, XPathExpressionException
    {
        OkHttpClient client = new OkHttpClient();
        String url = "https://community-open-weather-map.p.rapidapi.com/weather?q="+ciudad+"%2Ces&lat=0&lon=0&id=2172797&lang=null&units=metric&mode=JSON";
        Request request = new Request.Builder()
                .url(url)
                .get()
                .addHeader("x-rapidapi-host", "community-open-weather-map.p.rapidapi.com")
                .addHeader("x-rapidapi-key", apikey)
                .build();

        ResponseBody response = client.newCall(request).execute().body();
        String json = response.string();
        //System.out.println(json+"\n");

        JSONObject obj = new JSONObject(json);

        return obj;


        /*JSONArray arr = obj.getJSONArray("main");

        for (int i = 0; i < arr.length(); i++)
        {
            String post_id = arr.getJSONObject(i).getString("temp");
            System.out.println(post_id);
        }*/
    }
}
