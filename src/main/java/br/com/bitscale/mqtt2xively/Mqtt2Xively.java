package br.com.bitscale.mqtt2xively;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Mqtt2Xively {
	private static Logger logger = LogManager.getLogger(Mqtt2Xively.class.getName());

	public static void main(String[] args) {
    	logger.info("Iniciando aplicação.");
    	new MQTTSubscriber().exec();
	}

}
