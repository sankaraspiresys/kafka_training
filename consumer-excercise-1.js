const fs = require('fs')
const ip = require('ip')

const { Kafka, logLevel } = require('kafkajs')
const Message = require('./models/message.model.js');

const dbConfig = require('./config/database.config.js');
const mongoose = require('mongoose');
mongoose.Promise = global.Promise;
// Connecting to the database
mongoose.connect(dbConfig.url, {
  useNewUrlParser: true,
  useUnifiedTopology: true
}).then(() => {
  console.log("Successfully connected to the database");    
}).catch(err => {
  console.log('Could not connect to the database. Exiting now...', err);
  process.exit();
});

const host = process.env.DOCKER_HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:9092`,`${host}:9093`,`${host}:9094`],
  clientId: 'my-consumer'
})

const topic = 'message-topic'
const consumer = kafka.consumer({ groupId: 'consumer-group-1' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true  })
  
  await consumer.run({
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        for (let message of batch.messages) {
          const msgFromProducer = JSON.parse(JSON.parse(message.value));
          try {
            const msg = new Message({
              name: msgFromProducer.name,
              age: msgFromProducer.age,
              id: msgFromProducer.id
            });
    
            fs.appendFile('messages.txt', JSON.parse(message.value)+'\n', function (err) {
              if (err) throw err;
              console.log('Saved!');
            });
    
            msg.save()
              .then(data => {
                  console.log(data);
              }).catch(err => {
                console.log(err.message)
              })
    
            console.log("Writing to file / DB \n")
          } catch (e) {
              console.log("Something went wrong")
          }

          resolveOffset(message.offset)
          await heartbeat()
      }

      // Wait one sec after completing writing to DB / file
      consumer.pause([{ topic }])
      setTimeout(() => consumer.resume([{ topic }]),  1000)
      
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