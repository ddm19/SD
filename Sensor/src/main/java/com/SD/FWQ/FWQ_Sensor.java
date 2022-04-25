import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
import java.util.Scanner;

public class FWQ_Sensor extends Thread
{
    //Opciones: Elevar, Disminuir, Número Fijo

    public static void main (String[] args)
    {
		

        String ip = "";
        int id = -1,maxpersonas;
        if( args.length < 2)
        {
            System.out.println("Error en los argumentos");				// Tratamiento de Parámetros IP:Puerto + ID atracción
            System.out.println("Uso: IP:puertokafka IdAtracción");
            System.exit(1);
        }
        try
        {

            ip = args[0];
            id = Integer.parseInt(args[1]);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        maxpersonas = ConsultaMaxAtraccion(id);		//Obtengo MaxPersonas Atracción
        if(maxpersonas == -1)
        {
            System.out.println("Error, el id de la atracción no existe");
        }


        String bootstrapServers = ip;
        //Creamos las propiedades del Productor
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
        Thread t = null;
        HiloSensor s = null;
        int op = -1;

        while(op != 5)
        {
		   op = -1;
            System.out.print("\033[H\033[2J");
            System.out.flush();
            PrintMenu();
            op = SeleccionMenu();
            int nfijo = 0;
            if(op != 5)
            {
				if (op != 1)
				{
					Scanner sc = new Scanner(System.in);
					System.out.println( "Introduzca un número fijo: " );
					nfijo = sc.nextInt();
				}
                //System.out.println( "CONTINUO" );
                if(s != null && t != null)      // Cerramos el hilo, para crear otro con otra opción
                {
                    if(t.isAlive())
                    {
                        s.running = false;
                        s = null;
                        t = null;
                        
                        //System.out.println( "CREO PRODUCTOR" );
						//Creamos el productor
						KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
						s = new HiloSensor(producer,id,op-1,maxpersonas,nfijo);
						t = new Thread(s);
						t.start();
                    }
				}
				else
				{
					//System.out.println( "CREO PRODUCTOR" );
					//Creamos el productor
					KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
					s = new HiloSensor(producer,id,op-1,maxpersonas,nfijo);
					t = new Thread(s);
					t.start();
				}
            }
            else if(op == 5)
            {
                if(s != null && t != null)
                    if(t.isAlive())
                        s.running = false;

            }
        }

    }

    public static int ConsultaMaxAtraccion(int id)	//Devuelve Máximo de personas en la atracción del argumento
    {
        int max = -1;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("atracciones.txt"));
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        try // Cargamos info de las atracciones
        {

            String line = br.readLine();        // Formato -> ID:CICLO:VISPORCICLO:

            while(line != null)
            {
                String[] linea = line.split(":");
                String idcomprobar = linea[0];
                String visporciclo = linea[2];

                if(id == Integer.parseInt(idcomprobar))
                    max = Integer.parseInt(visporciclo)*3;
                line = br.readLine();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return max;
    }

    public static void PrintMenu()
    {
        System.out.println( "Bienvenido al menú de Sensores" );
        System.out.println( "Por Favor, Seleccione una de los siguientes modos: [1-4]" );
        System.out.println( "1 - Modo aleatorio" );
        System.out.println( "2 - Número Fijo" );
        System.out.println( "3 - Aumento constante desde Número Fijo" );
        System.out.println( "4 - Descenso constante desde Número Fijo (min 0)" );
        System.out.println( "5 - Apagar Sensor" );
    }

    public static int SeleccionMenu()
    {
        Scanner sc = new Scanner(System.in);
        int op = -1;
        boolean num = false;
        while(!num)		// Seleccionar en Menú
        {
            try
            {
                op = Integer.parseInt(System.console().readLine());
                num = true;
                if(op < 1 || op > 5)
                {
                    num = false;
                    System.out.println("Error al procesar la opción");
                    System.out.println( "Por Favor, Seleccione una de las opciones: [1-4]" );
                }
            }
            catch(Exception e)
            {
                num = false;
                System.out.println("Error al procesar la opción");
                System.out.println( "Por Favor, Seleccione una de las opciones: [1-4]" );

            }
        }

        return op;

    }

}
