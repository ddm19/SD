import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.IOException;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;	
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;

import java.util.Properties;

public class FWQ_Engine
{
    static final int TAMANOMAPA = 20;
    public static void main (String[] args)
    {
        String ipwait = "",ipkafka = "";
        int puertowait = -1;
        int maxvisitors = 0;
        if( args.length < 3)
        {
            System.out.println("Error en los argumentos");					// Tratamiento de Parámetros
            System.out.println("Uso: IP:puerto-Kafka IP:puerto-WaitingServer MáximoVisitantes");
            System.exit(1);
        }

        try
        {
            ipkafka = args[0];
            ipwait = PortSplitter(args[1]).x;
            puertowait = PortSplitter(args[1]).y;
            maxvisitors = Integer.parseInt(args[2]);
        }
        catch(Exception e)
        {
            System.out.println("Error: " + e);
        }
        Socket skcliente = null;
        try
        {
            skcliente = new Socket(ipwait, puertowait);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        Consumer<Long,String> consumidor = createConsumer(ipkafka,"posiciones");// Creamos un Consumidor suscrito al topic posiciones
        Producer<Long,String> productor = createProducer(ipkafka,"mapa","Engine");
        
        HiloEngine en = new HiloEngine(TAMANOMAPA,consumidor,productor);
        Thread t =  new Thread(en);    // Hilo de ejecución Engine;

        t.start();
        boolean primeraerror = true;
        
        /*Runtime.getRuntime().addShutdownHook(new Thread() {
		public void run() 
		{
		    System.out.println("Apagando Engine...");						//Detectar Ctrl+C
			escribeSocket(skcliente,"SALIR");
		    
		}
	    });*/
        
        while(true)	// Comenzamos Peticiones a WaitingTimeServer
        {
			if(skcliente == null)
			{
				try
				{
					skcliente = new Socket(ipwait, puertowait);
					primeraerror = true;
				}
				catch (IOException e)
				{
					if(primeraerror)
					{
						System.out.println("Servidor de tiempos no disponible");
						primeraerror = false;
					}
					//e.printStackTrace();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
			else
			{
				String returned = "";
				try
				{
					TimeUnit.SECONDS.sleep(7);
				}
				catch(Exception e)
				{
					System.out.println("ja");
				}
				System.out.println("LEPIDO");
				escribeSocket(skcliente,"TiemposEspera?");	// Escribo por el socket preguntando por los tiempos de espera
				try
				{
					skcliente.setSoTimeout(10000);
					boolean ok = false;
					while(!ok)		// Esperamos respuesta de Waiting o timeout
					{
						returned = leeSocket(skcliente, returned);
						if(returned.length() != 0)					//Recibimos la lista de tiempos de espera
						{
							en.ActualizaTiempos(returned);
							System.out.println(returned);
							ok = true;
							
						}
					}
				}
				catch(java.net.SocketException ex)
				{
					System.out.println("Servidor de Tiempos no disponible");
					skcliente = null;
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}	
        

    }
    public static void escribeSocket (Socket p_sk, String p_Datos)
    {
        try
        {
            OutputStream aux = p_sk.getOutputStream();
            DataOutputStream flujo= new DataOutputStream( aux );
            flujo.writeUTF(p_Datos);
        }
        catch (Exception e)
        {
            System.out.println("Error: " + e);
        }
        return;
    }
	public static String leeSocket (Socket p_sk, String p_Datos) throws Exception
    {
        try
        {
            InputStream aux = p_sk.getInputStream();
            DataInputStream flujo = new DataInputStream( aux );
            p_Datos = new String();
            p_Datos = flujo.readUTF();
        }
        catch (Exception e)
        {
            //System.out.println("Error: " + e);
        }
        return p_Datos;
    }
    public static HashMap<Character,Integer> ProcesaTiempos(String procesar)  // Devuelve un hashmap con el id de la atracción y su tiempo
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
    public static Tuple<String,Integer> PortSplitter(String input)	//Recibe un string y Devuelve una tupla con la ip y puerto separadas
    {
        Tuple devolv = new Tuple<String,Integer>();

        String ip = "";
        int puerto = -1;
        if (input.indexOf(':') > -1) // <-- does it contain ":"?
        {
            String[] arr = input.split(":");
            ip = arr[0];
            try
            {
                puerto = Integer.parseInt(arr[1]);
            }
            catch (NumberFormatException e)
            {
                e.printStackTrace();
            }
        }
        else
        {
            ip = input;
        }
        devolv.set(ip,puerto);

        return devolv;
    }
    private static Consumer<Long, String> createConsumer(String ipkafka, String topic)
    {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,ipkafka);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"movimientos");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
    private static Producer<Long, String> createProducer(String ipkafka, String topic, String id)
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipkafka);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, id);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<>(props);
    }

}
