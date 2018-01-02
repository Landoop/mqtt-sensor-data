package com.landoop.mqtt

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.io.InputStream

object JacksonJson {

  val mapper = ObjectMapper().registerModule(KotlinModule())

  fun <T> toJson(t: T): String = mapper.writeValueAsString(t)

  inline fun <reified T : Any> fromJson(json: String): T {
    return mapper.readValue<T>(json, T::class.java)
  }

  inline fun <reified T : Any> fromJson(stream: InputStream): T {
    return mapper.readValue<T>(stream, T::class.java)
  }

  fun asJson(input: InputStream): JsonNode = mapper.readTree(input)

  fun asJson(input: String): JsonNode = mapper.readTree(input)
}