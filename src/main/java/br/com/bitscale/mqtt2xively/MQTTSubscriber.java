package br.com.bitscale.mqtt2xively;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.xively.client.XivelyService;
import com.xively.client.http.api.DatapointRequester;
import com.xively.client.model.Datapoint;


public class MQTTSubscriber implements MqttCallback {
	
	private static Logger logger = LogManager.getLogger(MQTTSubscriber.class.getName());
	
	private XivelyPublisher publisher;
	private MqttClient client    = null;
    private String broker		 = "tcp://mqtt.bitscale.com.br:1883";
    private String clientId      = "oled";
    
	public MQTTSubscriber() { 
		publisher = new XivelyPublisher();
	}
	
    public void exec() {
        MemoryPersistence persistence = new MemoryPersistence();
        

        try {
            client = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            logger.info("Conectando ao broker "+ broker);
            client.connect(connOpts);
            logger.info("Cliente conetado ao broker.");
            client.subscribe("orange/sensor/#");
            logger.info("Inscrição no tópico orange/sensor/#");
            client.setCallback(this);
            logger.info("Callbacked.");            
        } catch(MqttException me) {
        	logger.info("reason "+me.getReasonCode());
        	logger.info("msg "+me.getMessage());
        	logger.info("loc "+me.getLocalizedMessage());
        	logger.info("cause "+me.getCause());
        	logger.info("excep "+me);
        } 
        
    	/*
        
        finally {
        	try {
        		logger.info("Desconectando o cliente mqtt.");
				client.disconnect();
			} catch (MqttException e) {
				logger.error("Erro finalizando conexão.", e);
			}
        }
			*/
        
    }
    
    public void connectionLost(Throwable cause) {
    	logger.error("Conexão perdida.", cause);
    	logger.info("Reiniciando.");
    	this.exec();
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {

		Integer feedId = 924982911;
	    DatapointRequester requester;
	    Datapoint data;
  
	    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
	    df.setTimeZone(TimeZone.getTimeZone("BRT"));
	    Date today = Calendar.getInstance().getTime(); 
	    String reportDate = df.format(today);
	    
	    logger.info("Mensagem recebida no topico "+ topic + ".");
	    
    	if(topic.toString().equals("orange/sensor/temperature")) {
			 
			//XivelyService.instance().datastream(feedId).get("temperature");
    		
		    data = new Datapoint();
		    data.setValue(new String(message.getPayload()));
		    data.setAt(reportDate);
		    requester = XivelyService.instance().datapoint(feedId, "temperature");
		    requester.create(data);    		
		    
    		// publisher.publish("temperature", message.getPayload());
    		
    	}
    	
    	if(topic.toString().equals("orange/sensor/moisture")) {
			//XivelyService.instance().datastream(feedId).get("moisture");
		    data = new Datapoint();
		    data.setValue(new String(message.getPayload()));
		    data.setAt(reportDate);
		    requester = XivelyService.instance().datapoint(feedId, "moisture");
		    requester.create(data);
    	}

    	if(topic.toString().equals("orange/sensor/luminosity")) {
			//XivelyService.instance().datastream(feedId).get("luminosity");
		    data = new Datapoint();
		    data.setValue(new String(message.getPayload()));
		    data.setAt(reportDate);
		    requester = XivelyService.instance().datapoint(feedId, "luminosity");
		    requester.create(data);    
    	}
    	
    }

	public void deliveryComplete(IMqttDeliveryToken arg0) {
		logger.info("Mensagem entregue com sucesso. "+ arg0);
	}
    
	
}

		