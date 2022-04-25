import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;


public class FWQ_Visitor
{
    public static void main (String[] args)
    {
        String ipreg = "",ipkafka = "";
        int puertoreg = -1;
        if( args.length < 2)
        {
            System.out.println("Error en los argumentos");					// Tratamiento de Parámetros
            System.out.println("Uso: IP:puerto-Registry IP:puerto-Kafka");
            System.exit(1);
        }

        try
        {
            ipkafka = args[1];
            ipreg = PortSplitter(args[0]).x;
            puertoreg = PortSplitter(args[0]).y;

        }
        catch(Exception e)
        {
            System.out.println("Error: " + e);

        }
		
		SeleccionMenu(ipreg,ipkafka,puertoreg);
		

    }
    private static void SeleccionMenu(String ipreg,String ipkafka,Integer puertoreg)
    {
		int op = -1;
		while(op != 5)
		{	
			boolean num = false;
			PrintMenu();
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
						System.out.println( "Por Favor, Seleccione una de las opciones: [1-5]" );
					}
				}
				catch(Exception e)
				{
					num = false;
					System.out.println("Error al procesar la opción");
					System.out.println( "Por Favor, Seleccione una de las opciones: [1-5]" );

				}
			}

			switch(op)
			{
				case 1:
					System.out.print("\033[H\033[2J");
					System.out.flush();
					CrearPerfil(puertoreg, ipreg);
					break;
				case 2:
					System.out.print("\033[H\033[2J");
					System.out.flush();
					EditarPerfil(puertoreg, ipreg);
					break;
				case 3:
					System.out.print("\033[H\033[2J");
					System.out.flush();
					EntrarParque(puertoreg,ipreg,ipkafka);
					break;
				case 4:
					System.out.print("\033[H\033[2J");
					System.out.flush();
					SalirParque(puertoreg, ipreg);
					break;
				case 5:
					System.exit(0);
			}
			op = -1;
		}
	}
    public static void PrintMenu()
    {
        System.out.println( "Bienvenido Visitante!" );
        System.out.println( "Por Favor, Seleccione una de las siguientes opciones: [1-5]" );
        System.out.println( "1 - Crear un nuevo Usuario" );
        System.out.println( "2 - Editar Usuario existente" );
        System.out.println( "3 - Entrada al parque" );
        System.out.println( "4 - Salir del Parque" );
        System.out.println( "5 - Salir" );

    }

    public static void CrearPerfil(int puerto,String ip) // Pido los datos y creo un usuario
    {
        Scanner sc = new Scanner(System.in);
        String id, nombre, pass;
        System.out.println("----- Creando un Usuario -----");
        System.out.println("Introduzca ID: ");
        id = sc.next();											// Pido datos de nuevo usuario
        System.out.println("Introduzca Nombre: ");
        nombre = sc.next();
        System.out.println("Introduzca Contraseña: ");
        pass = sc.next();

        //AÑADIR CONTROL DE ERRORES Y CHECKEO DE TODO

        try
        {
            Socket skcliente = new Socket(ip, puerto);
            escribeSocket(skcliente,"1,"+id+","+nombre+","+pass);	// Escribo por el socket usando 1(crear) | ID | NOMBRE | PASS

            String returned = leeSocket(skcliente,"");

            boolean ok = true;

            /*
             * if (returned = ALGOHASALIDOMAL)					// Reviso la salida
             * 	ok = false
             */

            if(ok)
                System.out.println("Cliente Creado Correctamente!");
            else
                System.out.println("Algo no ha salido como esperábamos"); //Añadir mensajes personalizados maybe(?) ej: ha fallado el id ya existe...

        }
        catch(Exception e)
        {
            System.out.println("Error: " + e);
        }

    }

    public static void EditarPerfil(int puerto,String ip) //Pido los datos y modifico un usuario
    {
        Scanner sc = new Scanner(System.in);
        String id, newnombre, pass, newpass;

        System.out.println("----- Editando un Usuario -----");
        System.out.println("Introduzca ID: ");
        id = sc.next();											// Pido datos de nuevo usuario
        System.out.println("Introduzca Contraseña: ");
        pass = sc.next();
        System.out.println("Introduzca Nuevo Nombre: ");
        newnombre = sc.next();
        System.out.println("Introduzca Nueva Contraseña: ");
        newpass = sc.next();

        //AÑADIR CONTROL DE ERRORES Y CHECKEO DE TODO

        try
        {
            Socket skcliente = new Socket(ip, puerto);
            escribeSocket(skcliente,"2,"+id+","+newnombre+","+pass);	// Escribo por el socket usando 2(modifcar) | ID | PASS | NEWNOMBRE | NEWPASS

            String returned = leeSocket(skcliente,"");

            boolean ok = true;

            /*
             * if (returned = ALGOHASALIDOMAL)					// Reviso la salida
             * 	ok = false
             */

            if(ok)
                System.out.println("Cliente Modificado Correctamente!");
            else
                System.out.println("Algo no ha salido como esperábamos"); //Añadir mensajes personalizados maybe(?) ej: ha fallado el id ya existe...

        }
        catch(Exception e)
        {
            System.out.println("Error: " + e.toString());
        }
    }

    public static void EntrarParque(int puerto, String ip, String ipkafka) //Pido datos y Entro al Parque
    {

        Scanner sc = new Scanner(System.in);
        String id, pass;

        System.out.println("----- Entrando al Parque -----");
        System.out.println("Introduzca ID: ");
        id = sc.next();											// Pido datos de nuevo usuario
        System.out.println("Introduzca Contraseña: ");
        pass = sc.next();

        //AÑADIR CONTROL DE ERRORES Y CHECKEO DE TODO
		//System.out.println("Solicitud de Entrada Correcta! Bienvenido al Parque!");
		//System.out.print("[....................]\n[.......3............]\n[....................]\n[....................]\n[.........i..........]\n[....................]\n[........l...........]\n");
				
        try
        {
            System.out.println("Conectando con el servidor de verificación...");
            Socket skcliente = new Socket(ip, puerto);
            escribeSocket(skcliente,"3,"+id+","+pass);	// Escribo por el socket usando 3(login) | ID | PASS

            boolean ok = false;
            String devuelto = "";
          
			skcliente.setSoTimeout(10000);
			String cadena = "";
			while(!ok)		// Esperamos respuesta de Registry o al timeout
			{
				cadena = leeSocket(skcliente, cadena);
				if (cadena.equals("aforocompleto"))
				{
					ok = true;
					devuelto = "aforocompleto";
				}
				else if (cadena.equals("incorrecto"))
				{
					ok = true;
					devuelto = "incorrecto";
				}
				else if(cadena.equals("timeout"))
				{
					break;
				}
				else if(cadena.length() != 0)
				{
					ok = true;
					devuelto = cadena;
				}
				
			}
			if(ok && devuelto.equals("incorrecto"))
				System.out.println("Lo sentimos, No hemos podido identificar correctamente el usuario");
            else if(ok)	// Login Correcto
            {
                if(devuelto.equals("aforocompleto"))
                {
                    System.out.println("Lo sentimos, el parque está a rebosar! Espere un poco a que se vacíe");
                }
                //System.out.println("Lo sentimos, el parque está cerrado actualmente, Prueba a hacer cola para cuando abra!");
                else
                {
					System.out.println("Accediendo al parque!");
                    Consumer<Long,String> consumidor = createConsumer(ipkafka,"mapa");// Creamos un Consumidor suscrito al topic mapa
                    Producer<Long,String> productor = createProducer(ipkafka,"posiciones",id);
                    
                    HiloVisitor vi = new HiloVisitor(consumidor,productor,id,0,0); // Creo un hilo con el consumidor para recibir el mapa y la posición inicial
                    Thread t = new Thread(vi);
                    t.start();    // Arranco y pongo el Mapa a imprimir
					
                    //Comienzo de movimiento
					
					System.out.print("\033[H\033[2J");
					System.out.flush();
					System.out.println("Inserte \"salir\" para salir del parque");
					String s = "";
					while(!s.equals("salir"))
					{
						s = System.console().readLine();
					}
					
                }

            }
        }
        catch(java.net.ConnectException ex)
        {
			System.out.println("Lo sentimos, el servidor de verificación no está disponible en estos momentos, pruebe de nuevo más tarde");
		}
        catch(Exception e)
        {
            System.out.println("Error: " + e);
        }

    }

    public static void SalirParque(int puerto,String ip) //Pido datos y Entro al Parque
    {
        Scanner sc = new Scanner(System.in);
        String id, pass;

        System.out.println("----- Saliendo del Parque -----");
        System.exit(0);

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
        catch (java.net.SocketTimeoutException ex)
        {
			System.out.println("Lo sentimos, el servidor de verificación no está disponible en estos momentos, pruebe de nuevo más tarde");
			return "timeout";
		}
        catch (Exception e)
        {
            System.out.println("Error: " + e);
        }
        return p_Datos;
    }

    /*
     * Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
     * o -1 si hay error.
     */
    private static void EscribeKakfa(String ip, String topic,String mensaje)
    {
        String bootstrapServers = ip;
        //Creamos las propiedades del Productor
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creamos el productor
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic,mensaje);    // Envío actualización de sensores: ID-GenteEnCola

        //enviar información
        producer.send(record);
        // Conexión Kafka Engine para recibir TokenSesión

        producer.flush();
        producer.close();
    }

    private static ConsumerRecords<String,String> EscuchaKakfa(String ip, String topic)
    {
        String bootstrapServers = ip;

        Logger logger = LoggerFactory.getLogger(FWQ_Visitor.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"visitantes");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(topic));

        ConsumerRecords<String,String> records;
        records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records)
            {
                logger.info("Key: "+record.key()+", Value: "+record.value());
                //logger.info("Partition: " + record.partition() + ", Offset: "+record.offset());
            } //Mostrar mensajes */

        return records;




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

    private static Consumer<Long, String> createConsumer(String ipkafka,String topic)
    {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,ipkafka);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"visitantes");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
    private static Producer<Long, String> createProducer(String ipkafka,String topic,String id)
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipkafka);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, id);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<>(props);
    }


}
