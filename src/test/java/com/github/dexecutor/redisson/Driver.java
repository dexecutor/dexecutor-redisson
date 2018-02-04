package com.github.dexecutor.redisson;

import org.redisson.config.Config;

/**
 * 
 * Terminal #1
 * mvn test-compile exec:java -Djava.net.preferIPv4Stack=true -Dexec.mainClass="com.github.dexecutor.redisson.Driver" -Dexec.classpathScope="test" -Dexec.args="s node-A"
 * 
 * Terminal #2
 * mvn test-compile exec:java -Djava.net.preferIPv4Stack=true -Dexec.mainClass="com.github.dexecutor.redisson.Driver" -Dexec.classpathScope="test" -Dexec.args="s node-B"
 * 
 * Terminal #3
 * mvn test-compile exec:java  -Dexec.classpathScope="test" -Djava.net.preferIPv4Stack=true -Dexec.mainClass="com.github.dexecutor.redisson.Driver" -Dexec.args="m node-C"
 * 
 * @author Nadeem Mohammad
 *
 */
public class Driver {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {			
			System.out.println("Invalid number of arguments");
		} else {			
			doStart(args);
			System.out.println("Ctrl+D/Ctrl+Z to stop.");
		}
	}

	private static void doStart(String[] args) {
		boolean isMaster = isMaster(args[0]);
		Config config = config();
		if (isMaster) {
			new RedissionMasterNode(args[1], config).execute();
		} else {
			new RedissonSlaveNode(args[1], config).start();
		}
	}

	private static Config config() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://127.0.0.1:6379");
		return config;
	}

	private static boolean isMaster(String string) {
		return string.equalsIgnoreCase("m");
	}
}
