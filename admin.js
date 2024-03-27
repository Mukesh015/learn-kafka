const {kafka} = require("./client")


async function init(){
    const admin = kafka.admin();
    console.log("Admin connecting...");
    admin.connect()
    .then("Admin connected successfully")
    .catch(err => console.log(err));
    await admin.createTopics({
        topics: [
            {
                topic: "bike-riders",
                numPartitions: 2,
            },
        ],
    });
    console.log("Topics created successfully");
    await admin.disconnect();
    console.log("Admin disconnected successfully");
}

init();