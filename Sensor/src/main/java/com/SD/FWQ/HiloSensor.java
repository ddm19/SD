import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class HiloSensor extends Thread
{
    final private KafkaProducer<String, String> producer;
    final private int id;
    final private int mode;
    final private int maxpersonas;
    private int nfijo;
    public volatile boolean running;

    public HiloSensor(KafkaProducer<String, String> producer, int id, int mode, int maxpersonas, int nfijo)
    {
        this.producer = producer;
        this.id = id;
        this.mode = mode;
        this.maxpersonas = maxpersonas;
        this.nfijo = nfijo;
        running = true;
    }

    public void run()
    {
        Random r = new Random();
        ProducerRecord<String, String> record = null;

        while(running)
        {
            try
            {
                TimeUnit.SECONDS.sleep(r.nextInt(2)+1);				// Cada 1-3 segundos
            }
            catch(Exception e)
            {
                e.printStackTrace();
                producer.flush();
                producer.close();
            }
            switch (mode)
            {
                case 0: // Random
                    record = new ProducerRecord<>("sensores", id+"-"+ActualizaPeople(maxpersonas));    // Envío actualización de sensores: ID-GenteEnCola
                    break;
                case 1: // Nº Fijo
                    record = new ProducerRecord<>("sensores", id+"-"+nfijo);    // Envío actualización de sensores: ID-GenteEnCola
                    break;
                case 2: // Aumento desde nfijo
                    record = new ProducerRecord<>("sensores", id+"-"+nfijo);    // Envío actualización de sensores: ID-GenteEnCola
                    nfijo++;
                    break;
                case 3: // Descenso desde nfijo
                    record = new ProducerRecord<>("sensores", id+"-"+nfijo);    // Envío actualización de sensores: ID-GenteEnCola
                    if(nfijo > 0)
                        nfijo--;
                    break;
            }
			//System.out.println(record.toString());
            //enviar información
            //System.out.println(id+"-"+nfijo);
            producer.send(record);
        }
        producer.flush();
        producer.close();
    }
    private static int ActualizaPeople(int max)	// Devuelve un número aleatorio de personas en la atracción según el máximo del argumento
    {
        Random r1 = new Random();

        return r1.nextInt(max);

    }

}
