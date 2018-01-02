package com.landoop.mqtt

data class Sensor(val id:String,
                  val temperature:Double,
                  val humidity:Double,
                  val timestamp:Long)