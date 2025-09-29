#include <WiFi.h>
#include <ETH.h>
#include <WiFiClient.h>
#include <WiFiServer.h>

#define ETH_CLK_MODE ETH_CLOCK_GPIO0_IN
#define ETH_PHY_POWER 16
#define MAX_CLIENTS 10
#define MAX_SUBSCRIPTIONS 10
#define CLIENT_BUFFER_SIZE 512
#define GLOBAL_BUFFER_SIZE 1024
#define MAX_TOPIC_LENGTH 128

WiFiServer mqttServer(1883);

struct Subscription {
  char topic[MAX_TOPIC_LENGTH];
  uint8_t topicLen;
};

struct ClientInfo {
  WiFiClient client;
  bool connected;
  bool slotInUse;
  Subscription* subscriptions;
  uint8_t subCount;
  uint8_t* buffer;
  uint16_t bufferPos;
};

ClientInfo clients[MAX_CLIENTS];
uint8_t globalBuffer[GLOBAL_BUFFER_SIZE];

void WiFiEvent(WiFiEvent_t event) {
  switch (event) {
    case ARDUINO_EVENT_ETH_START:
      Serial.println("ETH Started");
      ETH.setHostname("mqtt-broker");
      break;
    case ARDUINO_EVENT_ETH_CONNECTED:
      Serial.println("ETH Connected");
      break;
    case ARDUINO_EVENT_ETH_GOT_IP:
      Serial.print("MQTT Broker: ");
      Serial.print(ETH.localIP());
      Serial.println(":1883");
      break;
    default:
      break;
  }
}

inline bool fastTopicMatch(const char* sub, uint8_t subLen, const char* top, uint8_t topLen) {
  if (subLen == 1 && sub[0] == '#') return true;
  if (subLen == topLen && memcmp(sub, top, subLen) == 0) return true;

  const char* subEnd = sub + subLen;
  const char* topEnd = top + topLen;

  while (sub < subEnd && top < topEnd) {
    if (*sub == '+') {
      while (top < topEnd && *top != '/') top++;
      sub++;
      if (sub < subEnd && top < topEnd && *sub == '/' && *top == '/') {
        sub++;
        top++;
      }
    } else if (*sub == '#') {
      return true;
    } else if (*sub == *top) {
      sub++;
      top++;
    } else {
      return false;
    }
  }

  return (sub == subEnd && top == topEnd) || (sub < subEnd && *sub == '#');
}

void broadcastMessage(const char* topic, uint8_t topicLen, const char* payload, uint8_t payloadLen, int sender) {
  uint16_t msgLen = 4 + topicLen + payloadLen;

  globalBuffer[0] = 0x30;
  globalBuffer[1] = msgLen - 2;
  globalBuffer[2] = (topicLen >> 8) & 0xFF;
  globalBuffer[3] = topicLen & 0xFF;

  memcpy(globalBuffer + 4, topic, topicLen);
  memcpy(globalBuffer + 4 + topicLen, payload, payloadLen);

  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (!clients[i].slotInUse || !clients[i].connected) continue;

    bool shouldSend = false;
    for (int j = 0; j < clients[i].subCount; j++) {
      if (fastTopicMatch(clients[i].subscriptions[j].topic, clients[i].subscriptions[j].topicLen, topic, topicLen)) {
        shouldSend = true;
        break;
      }
    }

    if (shouldSend) {
      int written = clients[i].client.write(globalBuffer, msgLen);
      Serial.print("Broadcast to client ");
      Serial.print(i);
      Serial.print(" (");
      Serial.print(written);
      Serial.println(" bytes)");
    }
  }
}

void handleConnect(int clientIndex) {
  uint8_t connack[] = { 0x20, 0x02, 0x00, 0x00 };
  clients[clientIndex].client.write(connack, 4);
  clients[clientIndex].connected = true;
  Serial.println("Client " + String(clientIndex) + " connected");
}

void handlePublish(int clientIndex, uint8_t* buffer, uint16_t length) {
  uint16_t pos = 2;
  uint8_t topicLength = (buffer[pos] << 8) | buffer[pos + 1];
  pos += 2;

  char topic[MAX_TOPIC_LENGTH];
  memcpy(topic, buffer + pos, topicLength);
  pos += topicLength;

  uint8_t payloadLength = length - pos;
  char payload[256];
  memcpy(payload, buffer + pos, payloadLength);

  Serial.print("PUB: ");
  Serial.write((const uint8_t*)topic, topicLength);
  Serial.print(" = ");
  Serial.write((const uint8_t*)payload, payloadLength);
  Serial.println();

  broadcastMessage(topic, topicLength, payload, payloadLength, clientIndex);
}

void handleSubscribe(int clientIndex, uint8_t* buffer) {
  uint16_t pos = 4;
  uint8_t topicLength = (buffer[pos] << 8) | buffer[pos + 1];
  pos += 2;

  if (clients[clientIndex].subCount < MAX_SUBSCRIPTIONS && topicLength < MAX_TOPIC_LENGTH) {
    Subscription* sub = &clients[clientIndex].subscriptions[clients[clientIndex].subCount];
    memcpy(sub->topic, buffer + pos, topicLength);
    sub->topic[topicLength] = '\0';
    sub->topicLen = topicLength;
    clients[clientIndex].subCount++;

    Serial.print("SUB: ");
    Serial.print(clientIndex);
    Serial.print(" -> ");
    Serial.write((const uint8_t*)sub->topic, topicLength);
    Serial.println();
  }

  uint8_t suback[] = { 0x90, 0x03, buffer[2], buffer[3], 0x00 };
  clients[clientIndex].client.write(suback, 5);
}

inline void processMessage(int clientIndex, uint8_t* buffer, uint16_t length) {
  if (length < 2) return;

  uint8_t messageType = (buffer[0] >> 4) & 0x0F;

  switch (messageType) {
    case 1:
      handleConnect(clientIndex);
      break;
    case 3:
      handlePublish(clientIndex, buffer, length);
      break;
    case 8:
      handleSubscribe(clientIndex, buffer);
      break;
    case 12:
      {
        uint8_t pingresp[] = { 0xD0, 0x00 };
        clients[clientIndex].client.write(pingresp, 2);
      }
      break;
    case 14:
      clients[clientIndex].connected = false;
      clients[clientIndex].client.stop();
      break;
  }
}

void setup() {
  Serial.begin(115200);

  for (int i = 0; i < MAX_CLIENTS; i++) {
    clients[i].connected = false;
    clients[i].slotInUse = false;
    clients[i].subscriptions = (Subscription*)malloc(sizeof(Subscription) * MAX_SUBSCRIPTIONS);
    clients[i].subCount = 0;
    clients[i].buffer = (uint8_t*)malloc(CLIENT_BUFFER_SIZE);
    clients[i].bufferPos = 0;
  }

  pinMode(16, OUTPUT);
  digitalWrite(16, HIGH);

  WiFi.onEvent(WiFiEvent);
  ETH.begin(ETH_PHY_LAN8720, 1, 23, 18, 16, ETH_CLK_MODE);

  while (!ETH.linkUp()) {
    delay(100);
  }

  mqttServer.begin();
  mqttServer.setNoDelay(true);
  Serial.println("Fast MQTT Broker ready");
}

void loop() {
  WiFiClient newClient = mqttServer.available();
  if (newClient) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
      if (!clients[i].slotInUse) {
        clients[i].client = newClient;
        clients[i].slotInUse = true;
        clients[i].connected = false;
        clients[i].subCount = 0;
        clients[i].bufferPos = 0;
        Serial.println("New client slot: " + String(i));
        break;
      }
    }
  }

  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (!clients[i].slotInUse) continue;

    if (!clients[i].client.connected()) {
      if (clients[i].connected) {
        Serial.println("Client " + String(i) + " disconnected");
      }
      clients[i].connected = false;
      clients[i].slotInUse = false;
      clients[i].subCount = 0;
      clients[i].bufferPos = 0;
      continue;
    }

    int available = clients[i].client.available();
    if (available > 0) {
      int toRead = available > (CLIENT_BUFFER_SIZE - clients[i].bufferPos) ? (CLIENT_BUFFER_SIZE - clients[i].bufferPos) : available;
      int bytesRead = clients[i].client.readBytes(clients[i].buffer + clients[i].bufferPos, toRead);

      if (bytesRead > 0) {
        clients[i].bufferPos += bytesRead;

        uint16_t pos = 0;
        while (pos < clients[i].bufferPos) {
          if (clients[i].bufferPos - pos < 2) break;

          uint8_t remainingLength = clients[i].buffer[pos + 1];
          uint16_t messageLength = remainingLength + 2;

          if (pos + messageLength > clients[i].bufferPos) break;

          processMessage(i, clients[i].buffer + pos, messageLength);
          pos += messageLength;
        }

        if (pos > 0) {
          if (pos < clients[i].bufferPos) {
            memmove(clients[i].buffer, clients[i].buffer + pos, clients[i].bufferPos - pos);
          }
          clients[i].bufferPos -= pos;
        }
      }
    }
  }
}