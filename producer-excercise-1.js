const fs = require('fs')
const ip = require('ip')
const dummyjson = require('dummy-json');

const { Kafka, CompressionTypes, logLevel } = require('kafkajs')

const host = process.env.DOCKER_HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: [`${host}:9092`,`${host}:9093`,`${host}:9094`],
  clientId: 'my-producer',
})

const topic = 'message-topic'
const producer = kafka.producer()


const sendMessage = (i) => {
  const template = `{
    "name": "{{firstName}}",
    "age": "{{int 18 65}}",
    "id": ${i}
  }`;
  const randomJSONMessage = dummyjson.parse(template); 

  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [
        {
          "value": JSON.stringify(randomJSONMessage)
        }
      ]
    })
    .then(console.log)
    .catch(e => console.error(`[example/producer] ${e.message}`, e))
}

const run = async () => {
  await producer.connect()
  for(var i=0; i < 10000; i++){
    sendMessage(i);
  }
}

run().catch(e => console.error(`[example/producer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`)
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await producer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})