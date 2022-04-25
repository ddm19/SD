import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.HashMap;

public class HiloWaiting extends Thread
{

    private HashMap<String,Integer> atraccionesciclo;
    private HashMap<String,Integer> atraccionesvisitantesciclo;
    private HashMap<String,Integer> atraccionestiempo;
    private final Consumer<Long,String> consumidor;


    public HashMap<String, Integer> getAtraccionestiempo()
    {
        return atraccionestiempo;
    }

    public HiloWaiting(HashMap<String, Integer> atraccionesciclo, HashMap<String, Integer> atraccionesvisitantesciclo, Consumer<Long, String> consumidor)
    {
        this.atraccionesciclo = atraccionesciclo;
        this.atraccionesvisitantesciclo = atraccionesvisitantesciclo;
        this.consumidor = consumidor;
        atraccionestiempo = new HashMap<>();
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
    }

    private void runConsumer() throws InterruptedException
    {
        final Consumer<Long, String> consumer = consumidor;

        final int giveUp = 100;   int noRecordsCount = 0;

        while (true)
        {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp)
                    break;
                else
                    continue;
            }

            for (ConsumerRecord record : consumerRecords)
            {
				System.out.println(record.value().toString());  
                String[] recibido = record.value().toString().split("-");  //Separo ID [0] de Visitantes [1]
                int visitantes = Integer.parseInt(recibido[1]);

                //Actualizo los tiempos de espera
                atraccionestiempo.put(recibido[0],CalculaTiempo(visitantes,atraccionesciclo.get(recibido[0]),atraccionesvisitantesciclo.get(recibido[0])));

            }
            consumer.commitSync();
        }

    }
    private int CalculaTiempo(Integer visitantes,Integer ciclo, Integer visitantesporciclo)
    {
        return (visitantes/visitantesporciclo)*ciclo;
    }


}
