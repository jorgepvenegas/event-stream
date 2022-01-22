const redis = require("redis");
const mqtt = require("mqtt");

const redisClient = redis.createClient("redis://192.168.1.148:6379");
const mqttClient = mqtt.connect("mqtt://192.168.1.125:1883", {});
const postgres = require("postgres");
const sql = postgres("postgres://postgres:123456@localhost:5432/events");


const insertEntries = async (entries) => {
  if (entries === null) return;
  const { topic, value } = entries;
  let data = await sql`
  insert into entries (
    topic,
    event_time,
    value
  ) values (
    ${topic},
    CURRENT_TIMESTAMP,
    ${value}
  )`;
  console.log(data);
};


const topicsToSubscribe = [
  "bedroom/sensor/temperature/state",
  "bedroom/sensor/humidity/state",
  "livingroom/sensor/temperature/state",
  "livingroom/sensor/humidity/state",
];

mqttClient.on("connect", function () {
  console.log("MQTT Client connected");
  topicsToSubscribe.forEach(function (topic) {
    mqttClient.subscribe(topic, function (err) {
      if (!err) {
        console.log(`Subscribed to ${topic}`);
      }
    });
  });
});

mqttClient.on("message", function (topic, message) {
  // message is Buffer
  const data = {
    topic,
    timestamp: new Date().getTime(),
    value: Number(message.toString()),
  };

  // Create a unique key
  const key = `${data.topic}_${data.timestamp}`;

  redisClient.set(key, JSON.stringify(data));

  const insert = async (data) => {
    await insertEntries(data);
  };

  insert(data);
});

mqttClient.on("error", function (err) {
  console.log("Error", error);
});

mqttClient.on("end", function () {
  console.log("end");
});

