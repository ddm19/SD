import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FWQ_WaitingTimeServer
{

    public static void main(String[] args)
    {
        HashMap<String, Integer> atraccionesciclo = new HashMap<>(),atraccionesvisitantesciclo = new HashMap<>();
        Socket skCliente = null;
        String puerto="",ipkafka = "";
        try
        {
            if (args.length < 2) {
                System.out.println("Debe indicar el puerto de escucha del servidor y la IP:Puerto de Kafka");
                System.out.println("$./Servidor puerto_servidor IPKAFKA");
                System.exit (1);
            }
            puerto = args[0];
            ipkafka = args[1];

            Consumer<Long,String> consumidor = createConsumer(ipkafka,"sensores"); // Creo el consumidor para escuchar a los sensores

            BufferedReader br = new BufferedReader(new FileReader("atracciones.txt"));
            try // Cargamos info de las atracciones
            {
                String line = br.readLine();        // Formato -> ID:CICLO:VISPORCICLO:

                while(line != null)
                {
                    String[] linea = line.split(":");
                    String id = linea[0];
                    String ciclo = linea[1];
                    String visporciclo = linea[2];

                    atraccionesciclo.put(id,Integer.parseInt(ciclo));
                    atraccionesvisitantesciclo.put(id,Integer.parseInt(visporciclo));

                    line = br.readLine();
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            HiloWaiting w = new HiloWaiting(atraccionesciclo,atraccionesvisitantesciclo,consumidor);   // Recibo y actualizo tiempos de espera
			Thread t = new Thread(w);
            t.start();

            ServerSocket skServidor = new ServerSocket(Integer.parseInt(puerto));
			
			
            for(;;) 
            {
				Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() 
				{
					System.out.println("Apagando Servidor...");						//Detectar Ctrl+C
					try
					{
						skServidor.close();
					}
					catch(Exception e)
					{
					}
					
				}
				});
                String cadena = "";
                /*
                 * Se espera un cliente que quiera conectarse
                 */
                 
					skCliente = skServidor.accept(); // Crea objeto
					System.out.println("Sirviendo cliente..."+cadena);
					boolean ok = false;
					while(!ok)
					{
						cadena = "";
						try
						{
							cadena = leeSocket(skCliente, cadena);
						}
						catch(Exception e)
						{
							System.out.println("Cerrando cliente...");
						}
						if (cadena.equals("TiemposEspera?"))
						{
							String listatiempos = ConstruyeTiempos(w.getAtraccionestiempo());	// Creo una lista con la atracción y el tiempo de espera FORMATO-> ID1:TIEMPO1,ID2:TIEMPO2
							System.out.println("SI"+listatiempos);
							escribeSocket(skCliente, listatiempos);
						}
						else if(cadena.equals("SALIR"))
						{
							ok = true;
							skCliente.close();
						}
					}
					System.out.println("Engine Cerrado");


            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if(skCliente != null)
            {
                try {
                    skCliente.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private static String ConstruyeTiempos(HashMap<String,Integer> atraccionestiempo)	//FORMATO-> ID1:TIEMPO1,ID2:TIEMPO2
    {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String,Integer> set : atraccionestiempo.entrySet())   // Recorro todas las atracciones
        {
            sb.append(set.getKey());    // Añado el ID
            sb.append(":");
            sb.append(set.getValue()); // Añado el Tiempo;
            sb.append(",");

        }
        return sb.toString();
    }
    private static Consumer<Long, String> createConsumer(String ipkafka, String topic)
    {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,ipkafka);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"tiemposespera");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
   		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");


        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }

    public static String leeSocket (Socket p_sk, String p_Datos)
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
			//System.out.println("Motor del parque cerrado, apagando app...");
			//System.exit(0);
            //e.printStackTrace();
        }
        return p_Datos;
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


}
