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
  await consumer.subscribe({ topic, fromBeginning: true  })

  await consumer.run({
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      /* console.log(batch.topic)
      console.log(batch.partition)
      console.log(batch.highWatermark)
      console.log(batch.messages) */
        var count = 0;
        for (let message of batch.messages) {
            /* console.log({
                topic: batch.topic,
                partition: batch.partition,
                highWatermark: batch.highWatermark,
                message: {
                    offset: message.offset,
                    value: message.value.toString(),
                    headers: message.headers,
                }
            }) */
            try{
              resolveOffset(message.offset)
              await heartbeat()

              const msgFromProducer = JSON.parse(message.value.toString());
      
              console.log(msgFromProducer)

              let verify = function(s){ try { JSON.parse(msgFromProducer); return true; } catch (e) { return false; }}; 
              console.log(message.offset)
              console.log(verify())
      
              if(verify() == true){
            
                fs.appendFile('messages.txt', JSON.parse(message.value)+'\n', function (err) {
                  if (err) throw err;
                  console.log('Saved!');
                });
      
                console.log("Writing to file \n")
              }else{
                count ++;
                consumer.pause([{ topic }])
                setTimeout(() => consumer.resume([{ topic }]), count * 1000)
              }
            }catch(err){
              console.log(err)
            }
        }
        
    },
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