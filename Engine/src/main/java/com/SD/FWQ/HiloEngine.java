import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.ResponseBody;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.Scanner;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;


import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class HiloEngine extends Thread
{
    private HashMap<Character,Integer> tiempoatracciones;
    private HashMap<Character,Tuple<Integer,Integer>> atraccionesposi,visitantesposi;
    private final Consumer<Long, String> consumidor;
    private final Producer<Long, String> productor;
    private String mapa,lista;
    private final int tamanomapa;
    private boolean waitingup;
    private int qvisitantes;
    private float tempzonas[];

    public HiloEngine(int tamanomapa, Consumer<Long, String> consumidor, Producer<Long,String> productor)
    {
		tiempoatracciones = new HashMap<>();
		atraccionesposi = new HashMap<>();
        visitantesposi = new HashMap<>();
        CargarMapa();   //Cargo datos de atracciones
        mapa = ConstruyeMapa(); // Construyo el mapa con las atracciones
        this.tamanomapa = tamanomapa;
        this.consumidor = consumidor;
        this.productor = productor;
        this.qvisitantes = 0;
        //this.waitingup = waitingup;
        tempzonas = new float[5];
        for(int i = 1; i < tempzonas.length ; i++)
        {
            tempzonas[i] = ObtieneClima(ObtieneCiudad(i));
            ActualizaTemps(i);
        }

    }
    public void run()
    {
        try
        {

            runConsumer();

        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
			consumidor.close();
			System.out.println("DONE");
		}
			
    }
    private void runConsumer() throws InterruptedException
    {
        final Consumer<Long, String> consumer = consumidor;

        final int giveUp = 100;   int noRecordsCount = 0;

        while (true)
        {
			//visitantesposi = new HashMap<>();
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(3000);
			
			String returned = "";
            
            if (consumerRecords.count()==0)	//Mapa con 0 visitantes
            {
					//tiempoatracciones = ProcesaTiempos(returned);
					lista = ConstruyeLista();
					mapa = ConstruyeMapa();
					StringBuilder sb = new StringBuilder();
					sb.append(mapa);
					sb.append('_');
					sb.append(lista);
					System.out.println(sb.toString());
					ProducerRecord producerRecord = new ProducerRecord<Long,String>("mapa",sb.toString());    //Enviamos el MAPA+LISTA
					productor.send(producerRecord);
                /*noRecordsCount++;
				if (noRecordsCount > giveUp)
                    break;
                else
                    continue;*/
            } 
				for(ConsumerRecord record : consumerRecords)
				{
					//tiempoatracciones = ProcesaTiempos(returned);
					lista = ConstruyeLista();
					String recepcion = record.value().toString();
					ProcesaVisitante(recepcion);
					mapa = ConstruyeMapa();
					StringBuilder sb = new StringBuilder();
					sb.append(mapa);
					sb.append('_');
					sb.append(lista);
					ProducerRecord producerRecord = new ProducerRecord<Long,String>("mapa",sb.toString());    //Enviamos el MAPA+LISTA
					productor.send(producerRecord);
				}

            consumer.commitSync();
        }
    }

    private String ConstruyeMapa()  // Crea un Mapa con las atracciones y los visitantes que haya
    {

        StringBuilder sb = new StringBuilder();

        for(int i = 0; i < tamanomapa ; i++)
        {
            for(int j = 0; j < tamanomapa ; j++)
            {
				boolean puesto = false;
				if(visitantesposi != null)	// Coloco visitantes
				{
					
					for(Map.Entry<Character,Tuple<Integer,Integer>> set : visitantesposi.entrySet())
						if(set.getValue().x == i && set.getValue().y == j)
						{
							sb.append(set.getKey());
							puesto = true;
						}
						if(!puesto)
							sb.append('.');
				}
				if(atraccionesposi != null)	//Coloco Atracciones
				{
					for(Map.Entry<Character,Tuple<Integer,Integer>> set : atraccionesposi.entrySet())
						if(set.getValue().x == i && set.getValue().y == j)
						{
							sb.append(set.getKey());
							puesto = true;
						}
						if(!puesto)
							sb.append('.');
				}
				else
					sb.append('.');
			}
            sb.append('\n');
        }

        return sb.toString();
    }
    private String ConstruyeLista()
    {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<Character,Integer> set : tiempoatracciones.entrySet())   // Recorro todas las atracciones
        {
            sb.append(set.getKey()); //Añado el ID
            sb.append(':');
            sb.append(set.getValue()); // Añado el tiempo
            sb.append(',');
            Tuple<Integer,Integer> posicion = atraccionesposi.get(set.getKey());
            sb.append(posicion.x); // Añado la x
            sb.append('.');
            sb.append(posicion.y);  // Añado la y
            sb.append('-');
        }
        return sb.toString();
    }
    public void ActualizaTiempos(String tiempos)
    {
		//Recibimos la lista con el FORMATO-> ID1:TIEMPO1,ID2:TIEMPO2
		//lista = tiempos;
		String[] atrtiempos = tiempos.split(",");
		for(String tiempo : atrtiempos)
		{
			String[] division = tiempo.split(":");
			tiempoatracciones.put(division[0].charAt(0),Integer.parseInt(division[1]));
		}
	}
    public HashMap<Character,Integer> ProcesaTiempos(String procesar)  // Devuelve un hashmap con el id de la atracción y su tiempo
    {
        char id = '\0';
        HashMap<Character,Integer> procesado = new HashMap<>();

        for (int i = 0; i < procesar.length(); i++)
        {
            StringBuilder numero = new StringBuilder();
            id = procesar.charAt(i);
            i++;
            while (i < procesar.length() && procesar.charAt(i) != ',' )   // Cojo todos los números del tiempo
            {
                if (procesar.charAt(i) != ':')
                    numero.append(procesar.charAt(i));
                i++;
            }
            int truenumero = Integer.parseInt(numero.toString());   // Transformo el número en int
            procesado.put(id,truenumero);
        }
        return procesado;
    }
    private void ProcesaVisitante(String procesar)  // Modifica / Crea el visitante con la posición en el HashMap
    {
		System.out.println("Visitante: "+procesar);
        Tuple<Integer,Integer> posicion = new Tuple<>();
        char id = procesar.charAt(0);
        for (int i = 2; i < procesar.length(); i++)
        {
            StringBuilder numero = new StringBuilder();
            while (procesar.charAt(i) != '.' )   // Cojo todos los números de la 1era coordenada
            {
                numero.append(procesar.charAt(i));
                i++;
            }
            i++;
            int truenumero = Integer.parseInt(numero.toString());   // Transformo el número en int
            posicion.x = truenumero;
            numero = new StringBuilder();
            while (i < procesar.length() && procesar.charAt(i) != '.' )   // Cojo todos los números de la 2a coordenada
            {
                numero.append(procesar.charAt(i));
                i++;
            }
            truenumero = Integer.parseInt(numero.toString());   // Transformo el número en int
            posicion.y = truenumero;
        }
        if(visitantesposi.get(id) != null)
            Actualiza1(String.valueOf(visitantesposi.get(id).x),String.valueOf(visitantesposi.get(id).y),".");  //Eliminamos (si existe) el movimiento anterior
		if (posicion.x == 69 && posicion.y == 69)
			visitantesposi.remove(id);
		else
			visitantesposi.put(id,posicion);    // Añado el visitante junto a su posición al Hashmap

        ActualizaBDD(); // AProvecho para actualizar el mapa para la API en la BDD

    }
    private void CargarMapa() // Carga desde la BDD los datos de atracciones
    {
        String sql = "Select * from mapa";

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql))
        {
            Class.forName("org.sqlite.JDBC");
            while(rs.next())
            {
                if(rs.getString("id_atracciones") != null)  // Si hay una atracción
                {
                    Tuple<Integer,Integer> coords = new Tuple<>();
                    coords.x = rs.getInt("Fila");
                    coords.y = rs.getInt(("Columna"));
                    atraccionesposi.put(rs.getString("id_atracciones").charAt(0),coords);
                }
            }
        }
        catch(SQLException e)
        {
            System.out.println(e.getMessage());
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

    }
    private static void Actualiza1(String fila,String columna,String id)
    {
        String sql = "UPDATE truemapa SET Fila = ?, Columna = ?, id_atracciones = ? WHERE Fila = ? and Columna = ?";
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setString(1, fila);
            pstmt.setString(2, columna);
            pstmt.setString(3, id);
            pstmt.setString(4, fila);
            pstmt.setString(5, columna);

            // update
            pstmt.executeUpdate();
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
        }
    }

    private static HashMap<String,Character>adapta(HashMap<Character, Tuple<Integer, Integer>> map)
    {
        HashMap<String,Character> rev = new HashMap<>();

        for(Map.Entry<Character, Tuple<Integer, Integer>> entry : map.entrySet()) {
            String s = entry.getValue().x +","+ entry.getValue().y;
            rev.put(s, entry.getKey());
        }
        return rev;
    }
    private void ActualizaBDD()
    {
        String fila,columna;
        HashMap<String,Character> visi = adapta(visitantesposi);
        HashMap<String,Character> atracc = adapta(atraccionesposi);

        for(int i = 0; i < tamanomapa ; i++)
        {
            for(int j = 0; j < tamanomapa ; j++)
            {
                System.out.println(i+"-"+j);
                fila = String.valueOf(i);
                columna = String.valueOf(j);
                boolean puesto = false;
                if(visitantesposi != null)	// Coloco visitantes
                {
                    String s = i+","+j;

                    if(visi.get(s) != null)
                        Actualiza1(fila,columna, String.valueOf(visi.get(s)));
                    if(atracc.get(s) != null)
                    {
                        Character atraccion = atracc.get(s);
                        String aux = Integer.toString(tiempoatracciones.get(atraccion));
                        if(visi.get(s) != null)
                            aux = aux + String.valueOf(visi.get(s));

                        Actualiza1(fila, columna, aux);
                    }
                }

            }
        }


    }

    private void ActualizaTemps(int i)
    {
        String sqlb = "delete from temperaturas WHERE ciudad = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sqlb)) {

            // set the corresponding param
            pstmt.setString(1, ObtieneCiudad(i));
            pstmt.executeUpdate();
        }
        catch (SQLException e)
        {
        }
        String sqla = "INSERT INTO temperaturas(ciudad,temp) VALUES(?,?)";
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sqla)) {

            // set the corresponding param
            pstmt.setString(1, ObtieneCiudad(i));
            pstmt.setString(2, String.valueOf(tempzonas[i]));

            pstmt.executeUpdate();
        }
        catch (SQLException e)
        {
        }
    }
private static String ObtieneAPIKEY()
    {
        String clave = "";

        try
        {
            File myObj = new File("APIKEY.txt");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                clave = data;
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
    public static JSONObject http(String ciudad,String apikey) throws IOException, ParserConfigurationException, SAXException, XPathExpressionException
    {
        String trueciudad [] = ciudad.split(",");
        OkHttpClient client = new OkHttpClient();
        String segunda = "";
        if (trueciudad.length > 1)
        {
            segunda = "%2C"+trueciudad[1];
        }
        String url = "https://community-open-weather-map.p.rapidapi.com/weather?q="+trueciudad[0]+segunda+"&lat=0&lon=0&id=2172797&lang=null&units=metric&mode=JSON";
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

    }
    private static float ObtieneClima(String ciudad)   // Devuelve el JSON con el clima
    {

        float temp = -1;

        String apikey = LeeArchivo("APIKEY.txt",1);

        System.out.println(ciudad);

        try
        {
            JSONObject obj = http(ciudad,apikey);
            //System.out.println(obj);
            float pageName = obj.getJSONObject("main").getFloat("temp");
            temp = pageName;
            System.out.println(ciudad+"->"+pageName);
        }
        catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }
        return temp;
    }

    private static String ObtieneCiudad(int zona)
    {
        String ciudad = "";

        int i = 0;

        try
        {
            File myObj = new File("ciudades.txt");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine() && i < zona)
            {

                String data = myReader.nextLine();
                ciudad = data;
                i++;
            }
            myReader.close();
        }
        catch (FileNotFoundException e)
        {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        // System.out.println("--dkfmsljkjdfmfkjdnfkndfkvnkdfn"+ciudad);

        return ciudad;

    }



    private static Connection connect()
    {

		Connection connection = null;
		try
		{
		  Class.forName("org.sqlite.JDBC");

		  // create a database connection
		  connection = DriverManager.getConnection("jdbc:sqlite:BDD.db");
		  Statement statement = connection.createStatement();
		  statement.setQueryTimeout(10);  // set timeout to 10 sec.
		}
		catch (SQLException e)
		{
			e.printStackTrace();

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return connection;
	}
    
}
