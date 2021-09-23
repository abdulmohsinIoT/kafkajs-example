const { ProtocolParser, parseIMEI, Data, GPRS, FMB640Utils } = require('complete-teltonika-parser')
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

// initialize a new kafka client and initialize a producer from it
const kafka = new Kafka({ clientId, brokers })
const producer = kafka.producer()

// we define an async function that writes a new message each second
const produce = async () => {
	await producer.connect()
	let i = 0

	const packet = '000000000000025e8e040000017bdf37eef00100000000000000000000000000000000ef0011000800ef0100f00000150400450200010100030000fa0100fb00000300b5000000b600000042373a000300f10000a41300c70000000000100066f4860002000b000000d177f3796c000e00000000054b73110001018300222b3030303030302e303030302b303030303030302e303030302b3030302e3030302f0000017bdf37c7e00000000000000000000000000000000000fa0011000800ef0000f00000150000450200010100030000fa0100fb00000300b5000000b600000042373f000300f10000000000c70000000000100066f4860002000b0000000000000000000e00000000000000000001018300222b3030303030302e303030302b303030303030302e303030302b3030302e3030302f000000f9cebb06200100000000000000000000000000000000fc0012000900ef0000f00000150000450200010100030000fa0000fb0000fc00000300b5000000b6000000423740000300f10000000000c70000000000100066f4860002000b0000000000000000000e00000000000000000001018300222b3030303030302e303030302b303030303030302e303030302b3030302e3030302f0000017bdee7e6a800175f94070cd80523001c00a405000000000011000800ef0100f00000150500450100010100030000fa0100fb00000300b5000800b6000700423870000300f10000a41300c70000000000100066f4860002000b000000d177f3796c000e00000000054b73110001018300222b3231353438332e363833332b303339323133392e373833332b3030302e3032382f0400004e76'
    let data = new ProtocolParser(packet);

	// after the produce has connected, we start an interval timer
	setInterval(async () => {
		try {
			var pet = {kind: 'CAT', name: 'Albert',key: String(i)};
			// send a message to the configured topic with
			// the key and value formed from the current value of `i`
			await producer.send({
				topic,
				messages: [
					{
						key: String(i),
						value: type.toBuffer(pet),
					},
				],
			})

			// if the message is written successfully, log it and increment `i`
			console.log("writes: ", i)
			i++
		} catch (err) {
			console.error("could not write message " + err)
		}
	}, 1000)
}

module.exports = produce