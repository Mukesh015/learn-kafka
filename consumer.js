const { kafka } = require("./client");
const group = process.argv[2];

async function init() {
  const consumer = kafka.consumer({ groupId: group });
  await consumer.connect();
  console.log("consumer connected !");
  await consumer.subscribe({ topic: "bike-riders", fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      console.log(
        `Group : ${group} [${topic}]: partition:${partition}: message:${message.value.toString()}`
      );
    },
  });
}

init();
