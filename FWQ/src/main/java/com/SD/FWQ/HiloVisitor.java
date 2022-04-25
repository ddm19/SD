import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;



import java.awt.*;    
import javax.swing.*;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.Collectors;

public class HiloVisitor extends Thread
{
    private final String id;
    private final Consumer<Long,String> consumidor;
    private final Producer<Long,String> productor;
    private Tuple<Integer,Integer> posicion;
    private String recepcion;
    private final HashMap<Character,Integer> atraccionestiempo;
    private final HashMap<Character,Tuple<Integer,Integer>> atraccionesposi;
    private char atracciondestino;
    //private float zonas[];
    private JFrame f;
    private float tempzonas[];


    public HiloVisitor(Consumer<Long, String> consumidor, Producer<Long,String>productor, String id, int x, int y)
    {
        this.consumidor = consumidor;
        this.productor = productor;
        this.id = id;
        this.posicion = new Tuple<>();
        atraccionestiempo = new HashMap<>();
        atraccionesposi = new HashMap<>();
        this.posicion.x = x;
        this.posicion.y = y;
        this.atracciondestino = '\0';
        /*zonas = new float[5];
        for(int i = 1 ; i < zonas.length ;i++ )
            zonas[i] = ObtieneClima(ObtieneCiudad(i));*/
        f = new JFrame();

        tempzonas = new float[5];
        for(int i = 1; i < tempzonas.length ; i++)
        {
            tempzonas[i] = ObtieneClima(ObtieneCiudad(i));
        }
    }

    public void run()
    {
		Runtime.getRuntime().addShutdownHook(new Thread() {
		public void run() {
		    System.out.println("Saliendo del parque...");						//Detectar Ctrl+C
		    posicion.x = 69;
		    posicion.y = 69;
		    Mover(0,0);
		    
		}
	    });
	    
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
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(3000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp)
                    break;
                else
                    continue;
            }

            for (ConsumerRecord record : consumerRecords)
            {
				//System.out.println(record.value().toString());  
                recepcion = record.value().toString();
            }
            if(recepcion != null)
			{
				//System.out.println(recepcion);
				runner();   // Imprimo mapa y comienzo movimiento
			}
            consumer.commitSync();
            try
			{
				sleep(2000);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
        }
        
        

        
    }
    private void runner()
    {
        String analisis = recepcion;
        String[] divisor = analisis.split("_");
		if(divisor.length == 2)
		{
			System.out.print("\033[H\033[2J");
			System.out.flush();
			ProcesarLista(divisor[1]);
			ImprimeMapa(divisor[0]);

			runMovement();
		}

    }
    private void runMovement()
    {
        if(atracciondestino == '\0')  // Todavía no hemos escogido atracción
        {
            EscogeAtraccion();
        }
        else if(atraccionestiempo.get(atracciondestino) > 60) // El tiempo de la atracción ha cambiado a > 60
        {
            EscogeAtraccion();
        }
        else
        {
            // Movimiento hacia atracción
            Tuple<Integer,Integer> posiciondestino = atraccionesposi.get(atracciondestino);

            if(posicion.y < posiciondestino.y)  // Movimiento Norte
            {
                if(posicion.x < posiciondestino.x) //  Noreste
                    Mover(1,1);
                else if(posicion.x > posiciondestino.x) //  Noroeste
                    Mover(-1,1);
                else                                    // Ya estamos en x, Norte
                    Mover(0,1);
            }
            else if (posicion.y > posiciondestino.y) // Movimiento Sur
            {
                if(posicion.x < posiciondestino.x) //  Sureste
                    Mover(1,-1);
                else if(posicion.x > posiciondestino.x) //  Suroeste
                    Mover(-1,-1);
                else                                    // Ya estamos en x, Sur
                    Mover(0,-1);
            }
            else if (posicion.y == posiciondestino.y && posicion.x != posiciondestino.x)
            {
				if(posicion.x < posiciondestino.x) //  Este
                    Mover(1,0);
                else if(posicion.x > posiciondestino.x) //  Oeste
                    Mover(-1,0);
			}
            else    // Estamos en x,y hemos llegado
            {
                

                try
                {
					System.out.println("Llego a la atracción: " + atracciondestino + "Haciendo cola y montando durante: " + (atraccionestiempo.get(atracciondestino)+(posiciondestino.y + 4)) + " Segundos.");
					sleep((atraccionestiempo.get(atracciondestino))*1000);	// Esperamos la cola de la atracción
                    sleep((posiciondestino.y + 4)*1000);   // Esperamos el tiempo de la atracción (su coordenada y + 4 segundos)
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                // Reset de la atracción escogida
                atracciondestino = '\0';
            }
		}
		
    }
    private void Mover(int x, int y)
    {
        posicion.x += x;
        posicion.y += y;
        String valor = id+"-"+posicion.x+"."+posicion.y;
        ProducerRecord producerRecord = new ProducerRecord<Long,String>("posiciones",valor);    //Enviamos la posición nueva a Engine
        productor.send(producerRecord);
    }
    private int ObtieneZona(int i , int j)
    {
        int zona = 1;

        if(i < 10 && j < 10)  // Zona 1 arriba izq
            zona = 1;
        else if (i < 10)   // Zona 2 arriba pero derecha
            zona = 2;
        else if(j < 10)    // Zona 3 abajo izq
            zona = 3;
        else
            zona = 4;

        return zona;
    }
    private void EscogeAtraccion()
    {
		char[] ids = new char[atraccionestiempo.size()];
		int i = 0;
		for(Map.Entry<Character,Integer> set : atraccionestiempo.entrySet())	// Recorro las atracciones
		{
            int x = atraccionesposi.get(set.getKey()).x;
            int y = atraccionesposi.get(set.getKey()).y;
            int zona = ObtieneZona(x,y);
            float clima = ObtieneClima(ObtieneCiudad(zona));
            
			if(set.getValue() < 60 &&  clima >= 20 && clima <= 30)	// Guardo las q tienen menor tiempo que 60 y están entre 20 y 30 grados
			{
				ids[i] = set.getKey();
				i++;
			}
		}

        Random generator = new Random();
        atracciondestino = ids[generator.nextInt(ids.length)];        // Selecciono una atracción


    }
    private void ProcesarLista(String procesar) // Formato lista ID:TIEMPO,XX.YY-ID2:TIEMPO2,XX.YY
    {
        int truenumero = -1;
        char id;

        for(int i = 0; i < procesar.length() ; i++)
        {
            int x = 0,y = 0;
            StringBuilder numero = new StringBuilder();
            id = procesar.charAt(i);
            i++;
            while(procesar.charAt(i) != ',')   // Cojo todos los números del tiempo
            {
                if(procesar.charAt(i) != ':')
                    numero.append(procesar.charAt(i));
                i++;
            }
            i++;
            try
            {
                x = Character.getNumericValue(procesar.charAt(i));
                i++;
                if(procesar.charAt(i) != '.')
                {
                    x = x*10;
                    x += Character.getNumericValue(procesar.charAt(i));
                    i++;
                }
                i++;
                y = Character.getNumericValue(procesar.charAt(i));
                i++;
                if(i != procesar.length() && procesar.charAt(i) != '-')
                {
                    y = y*10;
                    y += Character.getNumericValue(procesar.charAt(i));
                    i++;
                }


                truenumero = Integer.parseInt(numero.toString());   // Transformo el número en int
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            Tuple coords = new Tuple<Integer,Integer>();
            coords.x = x;
            coords.y = y;
            atraccionestiempo.put(id,truenumero);  // Añado el par ID-TIEMPO al hashmap
            atraccionesposi.put(id,coords);        // Añado el par ID-POSICION al hashmap
        }

    }

    private static int getZona(int x, int y, int tam)
    {
        int zona = -1;

        if( y < tam/2 )
            if(x < tam/2)
                zona = 1;
            else
                zona = 3;
        else
        if(x < tam/2)
            zona = 2;
        else
            zona = 4;

        return zona;
    }


    private void ImprimeMapa(String procesar)
    {
        f.getContentPane().removeAll();
        f.repaint();

        for(int i = 0; i < procesar.length() ; i++)
        {
            JPanel panel = new JPanel();

            if (procesar.charAt(i) == '.')      // Carácter vacío de Mapa
                panel.setBackground(Color.white);
                //System.out.print('.');
            else if (Character.isLetter(procesar.charAt(i)))
            {
                panel.setBackground(new Color(0,0,255,80));
                panel.add(new JLabel(Character.toString(procesar.charAt(i))));
            }
            //System.out.print(procesar.charAt(i));    // ID visitante mapa
            else if (Character.isDigit(procesar.charAt(i)))
            {
                panel.setBackground(new Color(255,255,0,80));
                panel.add(new JLabel(atraccionestiempo.get(procesar.charAt(i)).toString()));
            }
            //System.out.print(atraccionestiempo.get(procesar.charAt(i)));  // Recupero el tiempo y lo imprimo
            else if (procesar.charAt(i) == '\n')
                panel.setBackground(Color.white);

            //System.out.println();                                   // Salto de línea

            f.add(panel);
        }

        f.setLayout(new GridLayout(22,22));
        f.setSize(600,600);
        f.setVisible(true);
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

    private float ObtieneClima(String ciudad)
    {
        String sql = "Select temp from temperaturas where ciudad = +\""+ciudad+"\"";
        float temp = 15;
        String aux = "15";
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();

             ResultSet rs = stmt.executeQuery(sql))
        {
            Class.forName("org.sqlite.JDBC");

            while (rs.next())
            {
                aux = rs.getString("temp");
            }
            temp = Float.valueOf(aux);

        }
        catch(SQLException e)
        {
            System.out.println(e.getMessage());
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return temp;
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
