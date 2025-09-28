#include <WiFi.h>
#include <ETH.h>
#include <WiFiClient.h>
#include <WiFiServer.h>
#include <vector>

#define ETH_CLK_MODE ETH_CLOCK_GPIO0_IN
#define ETH_PHY_POWER 16
#define MAX_CLIENTS 10
#define BUFFER_SIZE 1024

WiFiServer mqttServer(1883);

struct ClientInfo {
  WiFiClient client;
  bool connected;
  String* subscriptions;
  int subCount;
  uint8_t* buffer;
  int bufferPos;
};

ClientInfo clients[MAX_CLIENTS];
uint8_t globalBuffer[BUFFER_SIZE];

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

bool fastTopicMatch(const String& subscription, const String& topic) {
  if (subscription == "#") return true;
  if (subscription == topic) return true;
  
  const char* sub = subscription.c_str();
  const char* top = topic.c_str();
  
  while (*sub && *top) {
    if (*sub == '+') {
      while (*top && *top != '/') top++;
      sub++;
      if (*sub == '/' && *top == '/') {
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
  
  return (*sub == 0 && *top == 0) || (*sub == '#');
}

void broadcastMessage(const String& topic, const String& payload, int sender) {
  int topicLen = topic.length();
  int payloadLen = payload.length();
  int msgLen = 4 + topicLen + payloadLen;
  
  globalBuffer[0] = 0x30;
  globalBuffer[1] = msgLen - 2;
  globalBuffer[2] = (topicLen >> 8) & 0xFF;
  globalBuffer[3] = topicLen & 0xFF;
  
  memcpy(globalBuffer + 4, topic.c_str(), topicLen);
  memcpy(globalBuffer + 4 + topicLen, payload.c_str(), payloadLen);
  
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (i == sender || !clients[i].connected) continue;
    
    for (int j = 0; j < clients[i].subCount; j++) {
      if (fastTopicMatch(clients[i].subscriptions[j], topic)) {
        clients[i].client.write(globalBuffer, msgLen);
        break;
      }
    }
  }
}

void handleConnect(int clientIndex) {
  uint8_t connack[] = {0x20, 0x02, 0x00, 0x00};
  clients[clientIndex].client.write(connack, 4);
  clients[clientIndex].connected = true;
  Serial.println("Client " + String(clientIndex) + " connected");
}

void handlePublish(int clientIndex, uint8_t* buffer, int length) {
  int pos = 2;
  int topicLength = (buffer[pos] << 8) | buffer[pos + 1];
  pos += 2;
  
  String topic = "";
  topic.reserve(topicLength);
  for (int i = 0; i < topicLength; i++) {
    topic += (char)buffer[pos + i];
  }
  pos += topicLength;
  
  String payload = "";
  int payloadLength = length - pos;
  payload.reserve(payloadLength);
  for (int i = pos; i < length; i++) {
    payload += (char)buffer[i];
  }
  
  Serial.println("PUB: " + topic + " = " + payload);
  broadcastMessage(topic, payload, clientIndex);
}

void handleSubscribe(int clientIndex, uint8_t* buffer) {
  int pos = 4;
  int topicLength = (buffer[pos] << 8) | buffer[pos + 1];
  pos += 2;
  
  String topic = "";
  topic.reserve(topicLength);
  for (int i = 0; i < topicLength; i++) {
    topic += (char)buffer[pos + i];
  }
  
  if (clients[clientIndex].subCount < 10) {
    clients[clientIndex].subscriptions[clients[clientIndex].subCount] = topic;
    clients[clientIndex].subCount++;
  }
  
  uint8_t suback[] = {0x90, 0x03, buffer[2], buffer[3], 0x00};
  clients[clientIndex].client.write(suback, 5);
  
  Serial.println("SUB: " + String(clientIndex) + " -> " + topic);
}

void processMessage(int clientIndex, uint8_t* buffer, int length) {
  if (length < 2) return;
  
  uint8_t messageType = (buffer[0] >> 4) & 0x0F;
  
  switch (messageType) {
    case 1: handleConnect(clientIndex); break;
    case 3: handlePublish(clientIndex, buffer, length); break;
    case 8: handleSubscribe(clientIndex, buffer); break;
    case 12: 
      {
        uint8_t pingresp[] = {0xD0, 0x00};
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
    clients[i].subscriptions = new String[10];
    clients[i].subCount = 0;
    clients[i].buffer = new uint8_t[512];
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
      if (!clients[i].connected) {
        clients[i].client = newClient;
        clients[i].connected = false;
        clients[i].subCount = 0;
        Serial.println("New client slot: " + String(i));
        break;
      }
    }
  }
  
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (!clients[i].client.connected()) {
      if (clients[i].connected) {
        clients[i].connected = false;
        clients[i].subCount = 0;
        Serial.println("Client " + String(i) + " disconnected");
      }
      continue;
    }
    
    while (clients[i].client.available()) {
      int bytesRead = clients[i].client.readBytes(clients[i].buffer, 512);
      if (bytesRead > 0) {
        processMessage(i, clients[i].buffer, bytesRead);
      }
    }
  }
}