// import the `Kafka` instance from the kafkajs library
const { Kafka } = require("kafkajs")

var avro = require('avro-js');
// We can declare a schema inline:
var type = avro.parse({
	name: 'Pet',
	type: 'record',
	fields: [
	  {name: 'kind', type: {name: 'Kind', type: 'enum', symbols: ['CAT', 'DOG']}},
	  {name: 'name', type: 'string'},
	  {name: 'key', type: 'string'}
	]
  });

// the client ID lets kafka know who's producing the messages
const clientId = "kafka1"
// we can define the list of brokers in the cluster
const brokers = ["localhost:9092"]
// this is the topic to which we want to write messages
const topic = "test"
// the kafka instance and configuration variables are the same as before

// create a new consumer from the kafka client, and set its group ID
// the group ID helps Kafka keep track of the messages that this client
// is yet to receive
const kafka = new Kafka({ clientId, brokers })
const consumer = kafka.consumer({ groupId: clientId })

const consume = async () => {
	// first, we wait for the client to connect and subscribe to the given topic
	await consumer.connect()
	await consumer.subscribe({ topic })
	await consumer.run({
		// this function is called every time the consumer gets a new message
		eachMessage: ({ message }) => {
			// here, we just log the message to the standard output
			console.log({
				key: message.key.toString(),
				value: type.fromBuffer(message.value)
			  });
		},
	})
}

module.exports = consume