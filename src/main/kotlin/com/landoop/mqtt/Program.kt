package com.landoop.mqtt

//import io.moquette.server.config.ClasspathConfig
//import io.netty.handler.codec.mqtt.*
import io.moquette.BrokerConstants
import io.moquette.interception.AbstractInterceptHandler
import io.moquette.interception.messages.InterceptPublishMessage
import io.moquette.server.Server
import io.moquette.server.config.MemoryConfig
import io.netty.handler.codec.mqtt.MqttQoS
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import java.util.*
import kotlin.system.exitProcess


fun main(args: Array<String>): Unit {
  if (args.size != 2) {
    println("""
      Usage: PORT TOPIC
      Missing the port number for the MQTT broker.""".trimIndent())
    exitProcess(100)
  }

  val port = args[0].toIntOrNull()
  if (port == null) {
    println("""Invalid port number. ${args[0]} is not a valid integer.""".trimIndent())
    exitProcess(100)
  }

  val topic = args[1].trim()
  if (topic.isEmpty()) {
    println("""Invalid topic name. ${args[1]} is not a valid topic to publish the data.""".trimIndent())
    exitProcess(100)
  }

  println("Starting the Moquete broker on port $port")
  val mqttBroker = Server()
  val properties = Properties()
  properties.put(BrokerConstants.PORT_PROPERTY_NAME, "$port")
  properties.put(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "true")
  properties.put(BrokerConstants.HOST, "0.0.0.0")
  properties.put(BrokerConstants.HOST_PROPERTY_NAME, "localhost")

  val config = MemoryConfig(properties)
  mqttBroker.startServer(config, listOf(object : AbstractInterceptHandler() {
    override fun getID(): String {
      return "EmbeddedLauncherPublishListener";
    }

    override fun onPublish(msg: InterceptPublishMessage?) {
      val bytes = ByteArray(msg!!.payload.readableBytes())
      msg!!.payload.readBytes(bytes)
      println("Received message on ${msg?.topicName} with payload :${String(bytes)}")
    }

  }))

  println("Press `CTRL-C` to stop ...")

  val sensorIds = arrayOf("SB01", "SB02", "SB03", "SB04")
  val random = Random(System.currentTimeMillis())

  val brokerUrl = "tcp://127.0.0.1:$port"
  val mqttClient = MqttClient(brokerUrl, "client1", MemoryPersistence())
  val connOpts = MqttConnectOptions()
  connOpts.isCleanSession = true
  println("paho-client connecting to broker: " + brokerUrl)
  mqttClient.connect(connOpts)
  println("paho-client connected to broker")

  Runtime.getRuntime().addShutdownHook(object : Thread() {
    override fun run() {
      println("Stopping the Moquete broker...")
      mqttClient.disconnect()
      mqttBroker.stopServer()
      println("moquette mqtt broker stopped")
    }
  })


  fun publishSensorData(topic: String, payload: ByteArray): Unit {
    /* val message = MqttMessageBuilders.publish()
         .topicName("/exit")
         .retained(true)
         //        qos(MqttQoS.AT_MOST_ONCE);
         //        qQos(MqttQoS.AT_LEAST_ONCE);
         .qos(MqttQoS.EXACTLY_ONCE)
         .payload(Unpooled.copiedBuffer(payload))
         .build()
     mqttBroker.internalPublish(message, "client1")*/


    val message = org.eclipse.paho.client.mqttv3.MqttMessage(payload)
    message.setQos(0)
    mqttClient.publish(topic, message)
  }

  fun rand(from: Int, to: Int): Int {
    return random.nextInt(to - from) + from
  }

  fun generateSensorData(sensorId: String, prev: Sensor): Sensor {
    return Sensor(sensorId,
        prev.temperature + random.nextDouble() * 2 + rand(0, 2),
        prev.humidity + random.nextDouble() * 2 * (if (rand(0, 9) % 2 == 0) -1 else 1),
        System.currentTimeMillis())
  }

  val dataMap = sensorIds.map { it ->
    val sensor = Sensor(it, 23.0, 38.0, System.currentTimeMillis())
    it to sensor
  }.toMap()

  try {
    while (true) {
      sensorIds.forEach { sensorId ->
        val data = generateSensorData(sensorId, dataMap.get(sensorId)!!)
        val json = JacksonJson.toJson(data)
        //println("Publishing data: $json")
        publishSensorData(topic, json.toByteArray())
      }
      Thread.sleep(500)
    }
  } catch (e: Exception) {
  }

}