const fs = require('fs')
const ip = require('ip')

const { Kafka, logLevel } = require('kafkajs')


const host = process.env.DOCKER_HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:9092`,`${host}:9093`,`${host}:9094`],
  clientId: 'my-consumer'
})

const topic = 'message-topic1'
const consumer = kafka.consumer({ groupId: 'consumer-group-1' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: false  })

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      const msgFromProducer = JSON.parse(message.value.toString());
      consumer.commitOffsets([
        { topic: topic, partition: 0, offset: parseInt(message.offset) + 1 }
      ])
      let verify = function(s){ try { JSON.parse(msgFromProducer); return true; } catch (e) { return false; }}; 
      console.log("Offset ==> "+message.offset)
      if(verify() == true){
        fs.appendFile('messages.txt', JSON.parse(message.value)+'\n', function (err) {
          if (err) throw err;
        });
        console.log("JSON Message writing to file \n")
      }else{
        console.log("Not JSON Message, So disconnected...")
        await consumer.stop()
        await consumer.disconnect()
      }
    }
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})