/*
 * Smart Home - Bathroom ESP32 Node
 * FIXED: Servo issues resolved - Now stable!
 * 
 * Controls (Manual only via web):
 * - Shower (GPIO2) - Relay
 * - Heater (GPIO4) - Relay
 * - Smart Mirror (GPIO19) - Servo (with fixes)
 * - Lights (GPIO18) - Relay
 * 
 * Sensors (Reporting only - web handles alerts):
 * - Temperature Sensor (GPIO32) - DHT22
 * 
 * WiFi: KWIZERA / Kinno@123
 * Backend: http://192.168.137.1:8000/api
 * Room ID: 3 (Bathroom)
 * 
 * Device IDs:
 * - Shower: 18 (Controllable - Relay)
 * - Heater: 19 (Controllable - Relay)
 * - Smart Mirror: 20 (Controllable - Servo)
 * - Lights: 21 (Controllable - Relay)
 * - Temperature Sensor: 22 (Sensor - DHT22)
 */

#include <WiFi.h>
#include <HTTPClient.h>
#include <ArduinoJson.h>
#include <DHT.h>
#include <ESP32Servo.h>

// ==================== WiFi Configuration ====================
const char* ssid = "KWIZERA";
const char* password = "Kinno@123";
const char* serverUrl = "http://192.168.137.1:8000/api";
const int ROOM_ID = 3;  // Bathroom

// ==================== Pin Definitions ====================
#define RELAY_SHOWER_PIN 2
#define RELAY_HEATER_PIN 4
#define SERVO_MIRROR_PIN 12
#define RELAY_LIGHTS_PIN 18
#define DHTPIN 32
#define DHTTYPE DHT11

// ==================== Device IDs ====================
const int SHOWER_ID = 18;
const int HEATER_ID = 19;
const int MIRROR_ID = 20;
const int LIGHTS_ID = 21;
const int TEMP_SENSOR_ID = 22;

// ==================== TIMING ====================
const long POLL_INTERVAL = 300;      // Increased to 300ms for stability
const long SENSOR_INTERVAL = 3000;   // 3 seconds for sensor updates
const int WIFI_MAX_ATTEMPTS = 20;

// ==================== Relay Logic ====================
const bool RELAY_ACTIVE_LOW = false;   // false = HIGH turns relay ON

// ==================== Servo Positions ====================
const int MIRROR_ANGLE_OFF = 0;    // Mirror retracted/off position
const int MIRROR_ANGLE_ON = 90;    // Mirror extended/on position

// ==================== FIX: Servo Management ====================
bool servoMoving = false;
unsigned long servoMoveStartTime = 0;
const int SERVO_MOVE_DURATION = 300;  // Time to allow servo to move (ms)

// ==================== Global Objects ====================
WiFiClient wifiClient;
HTTPClient http;
DHT dht(DHTPIN, DHTTYPE);
Servo mirrorServo;

// Device states
bool showerState = false;
bool heaterState = false;
bool mirrorState = false;  // false = retracted, true = extended
bool lightsState = false;

// Target states (for servo)
bool targetMirrorState = false;

// Current sensor values
float lastTemperature = -1;
float lastHumidity = -1;

// Timing variables
unsigned long lastPoll = 0;
unsigned long lastSensorRead = 0;
unsigned long lastWiFiCheck = 0;

// ==================== Function Prototypes ====================
void printHeader();
void initPins();
bool connectToWiFi();
void checkWiFi();
void pollDeviceStates();
void parseDevices(String payload);
void controlDevice(int deviceId, String command);
void setRelay(int pin, bool state);
void setMirror(bool state);
void updateServo();  // New function for servo updates
void readAndSendSensors();
void sendSensorData(int sensorId, String value);
void printSystemInfo();

// ==================== Setup ====================
void setup() {
  Serial.begin(115200);
  delay(1000);
  
  printHeader();
  initPins();
  
  // Initialize DHT22 sensor
  dht.begin();
  Serial.println("  ✓ DHT22 Temperature Sensor initialized");
  
  // FIX: Configure WiFi for stability
  WiFi.setSleep(true);  // Enable WiFi sleep to reduce power consumption
  WiFi.setTxPower(WIFI_POWER_13dBm);  // Reduce WiFi power for stability
  
  if (connectToWiFi()) {
    printSystemInfo();
  }
  
  // Configure HTTP client
  http.setTimeout(1000);
  http.setReuse(true);
  
  Serial.println("\n✅ Setup complete! Manual control only - Web handles alerts\n");
  delay(100);
}

// ==================== Main Loop ====================
void loop() {
  unsigned long now = millis();
  
  // FIX: Update servo state (non-blocking)
  updateServo();
  
  // Quick WiFi check (less frequent)
  if (now - lastWiFiCheck >= 30000) {  // Check every 30 seconds
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("❌ WiFi disconnected! Reconnecting...");
      WiFi.reconnect();
    }
    lastWiFiCheck = now;
  }
  
  if (WiFi.status() == WL_CONNECTED) {
    // Poll for web commands
    if (now - lastPoll >= POLL_INTERVAL) {
      pollDeviceStates();
      lastPoll = now;
    }
    
    // Send sensor data
    if (now - lastSensorRead >= SENSOR_INTERVAL) {
      readAndSendSensors();
      lastSensorRead = now;
    }
  }
  
  delay(10);  // Small delay for stability (increased from 5ms)
}

// ==================== Print Header ====================
void printHeader() {
  Serial.println("\n\n╔══════════════════════════════════════════╗");
  Serial.println("║     SMART HOME - BATHROOM NODE v1.1    ║");
  Serial.println("║          SERVO STABLE VERSION          ║");
  Serial.println("╠══════════════════════════════════════════╣");
  Serial.println("║  Shower: GPIO2 (Relay)                  ║");
  Serial.println("║  Heater: GPIO4 (Relay)                  ║");
  Serial.println("║  Mirror: GPIO13 (Servo - FIXED)         ║");
  Serial.println("║  Lights: GPIO18 (Relay)                 ║");
  Serial.println("║  Temperature: GPIO32 (DHT22)            ║");
  Serial.println("║  POLLING: 300ms | SENSOR: 3s            ║");
  Serial.println("║  SERVO: Non-blocking control            ║");
  Serial.println("╚══════════════════════════════════════════╝");
  Serial.println();
}

// ==================== Initialize Pins ====================
void initPins() {
  Serial.println("\n📌 Initializing bathroom pins...");
  
  pinMode(RELAY_SHOWER_PIN, OUTPUT);
  pinMode(RELAY_HEATER_PIN, OUTPUT);
  pinMode(RELAY_LIGHTS_PIN, OUTPUT);
  
  // FIX: Initialize mirror servo with detach after setting
  mirrorServo.attach(SERVO_MIRROR_PIN);
  mirrorServo.write(MIRROR_ANGLE_OFF);  // Start retracted
  delay(100);  // Allow servo to reach position
  mirrorServo.detach();  // DETACH to save resources and prevent interference
  Serial.println("  ✓ Mirror servo initialized (RETRACTED) - DETACHED");
  
  // Start with all relays OFF
  if (RELAY_ACTIVE_LOW) {
    digitalWrite(RELAY_SHOWER_PIN, HIGH);
    digitalWrite(RELAY_HEATER_PIN, HIGH);
    digitalWrite(RELAY_LIGHTS_PIN, HIGH);
    Serial.println("  ✓ Relays initialized (OFF) - Active LOW");
  } else {
    digitalWrite(RELAY_SHOWER_PIN, LOW);
    digitalWrite(RELAY_HEATER_PIN, LOW);
    digitalWrite(RELAY_LIGHTS_PIN, LOW);
    Serial.println("  ✓ Relays initialized (OFF) - Active HIGH");
  }
  
  Serial.println();
}

// ==================== Connect to WiFi ====================
bool connectToWiFi() {
  Serial.print("📡 Connecting to WiFi: ");
  Serial.println(ssid);
  
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(true);  // Enable sleep for stability
  WiFi.setTxPower(WIFI_POWER_13dBm);  // Reduce power
  WiFi.begin(ssid, password);
  
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED && attempts < WIFI_MAX_ATTEMPTS) {
    delay(500);
    Serial.print(".");
    attempts++;
  }
  
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\n✅ WiFi Connected!");
    return true;
  } else {
    Serial.println("\n❌ WiFi Connection Failed!");
    return false;
  }
}

// ==================== Print System Info ====================
void printSystemInfo() {
  Serial.println("\n📊 System Information:");
  Serial.printf("   IP Address: %s\n", WiFi.localIP().toString().c_str());
  Serial.printf("   MAC Address: %s\n", WiFi.macAddress().c_str());
  Serial.printf("   RSSI: %d dBm\n", WiFi.RSSI());
  Serial.printf("   Free Heap: %d bytes\n", ESP.getFreeHeap());
  Serial.println();
}

// ==================== Poll Device States ====================
void pollDeviceStates() {
  String url = String(serverUrl) + "/devices/room/" + String(ROOM_ID);
  
  http.begin(wifiClient, url);
  http.setTimeout(500);
  
  int httpCode = http.GET();
  
  if (httpCode == HTTP_CODE_OK) {
    String payload = http.getString();
    parseDevices(payload);
  }
  
  http.end();
}

// ==================== Parse Devices JSON ====================
void parseDevices(String payload) {
  StaticJsonDocument<2048> doc;
  DeserializationError error = deserializeJson(doc, payload);
  
  if (error) {
    return;
  }
  
  JsonArray devices = doc.as<JsonArray>();
  
  for (JsonVariant device : devices) {
    int id = device["id"];
    const char* state = device["current_state"];
    bool isSensor = device["is_sensor"];
    
    // Control only non-sensor devices
    if (!isSensor && state != nullptr) {
      controlDevice(id, String(state));
    }
  }
}

// ==================== Control Device ====================
void controlDevice(int deviceId, String command) {
  bool newState = (command == "ON");
  
  switch(deviceId) {
    case SHOWER_ID:
      if (showerState != newState) {
        setRelay(RELAY_SHOWER_PIN, newState);
        showerState = newState;
        Serial.printf("🚿 Shower: %s (from web)\n", newState ? "ON" : "OFF");
      }
      break;
      
    case HEATER_ID:
      if (heaterState != newState) {
        setRelay(RELAY_HEATER_PIN, newState);
        heaterState = newState;
        Serial.printf("🔥 Heater: %s (from web)\n", newState ? "ON" : "OFF");
      }
      break;
      
    case MIRROR_ID:
      if (mirrorState != newState) {
        targetMirrorState = newState;  // Set target, actual move happens in updateServo()
        mirrorState = newState;  // Update logical state immediately
        Serial.printf("🪞 Smart Mirror: %s command received\n", newState ? "EXTEND" : "RETRACT");
      }
      break;
      
    case LIGHTS_ID:
      if (lightsState != newState) {
        setRelay(RELAY_LIGHTS_PIN, newState);
        lightsState = newState;
        Serial.printf("💡 Bathroom Lights: %s (from web)\n", newState ? "ON" : "OFF");
      }
      break;
  }
}

// ==================== Set Relay ====================
void setRelay(int pin, bool state) {
  if (RELAY_ACTIVE_LOW) {
    digitalWrite(pin, state ? LOW : HIGH);
  } else {
    digitalWrite(pin, state ? HIGH : LOW);
  }
}

// ==================== FIXED: Set Mirror Servo (Non-blocking) ====================
void setMirror(bool state) {
  // This function now just sets target state
  // Actual movement happens in updateServo()
  targetMirrorState = state;
  
  if (!servoMoving) {
    // Start servo movement
    mirrorServo.attach(SERVO_MIRROR_PIN);
    servoMoving = true;
    servoMoveStartTime = millis();
    
    if (state) {
      mirrorServo.write(MIRROR_ANGLE_ON);
    } else {
      mirrorServo.write(MIRROR_ANGLE_OFF);
    }
    
    Serial.printf("  Servo moving to %s position\n", state ? "EXTENDED" : "RETRACTED");
  }
}

// ==================== NEW: Update Servo State ====================
void updateServo() {
  if (servoMoving) {
    // Check if servo has had enough time to move
    if (millis() - servoMoveStartTime >= SERVO_MOVE_DURATION) {
      // Detach servo to save power and prevent interference
      mirrorServo.detach();
      servoMoving = false;
      Serial.println("  Servo movement complete - DETACHED");
    }
  }
  
  // Check if we need to start a new servo movement
  if (!servoMoving && (mirrorState != targetMirrorState)) {
    mirrorState = targetMirrorState;
    setMirror(mirrorState);
  }
}

// ==================== Read and Send Sensors ====================
void readAndSendSensors() {
  // Skip sensor reading if servo is moving (to prevent interference)
  if (servoMoving) {
    return;
  }
  
  // Read DHT22 sensor
  float humidity = dht.readHumidity();
  float temperature = dht.readTemperature();
  
  // Check if any reads failed
  if (isnan(humidity) || isnan(temperature)) {
    Serial.println("❌ Failed to read from DHT22 sensor!");
    return;
  }
  
  // Only send if temperature changed significantly (>0.5°C)
  if (abs(temperature - lastTemperature) > 0.5) {
    // Format temperature with unit
    char tempStr[20];
    snprintf(tempStr, sizeof(tempStr), "%.1f°C", temperature);
    
    sendSensorData(TEMP_SENSOR_ID, String(tempStr));
    
    // Determine if temperature is high/low for visual feedback
    if (temperature > 30) {
      Serial.printf("🌡️ Temperature: %.1f°C [HIGH] - Sent\n", temperature);
    } else if (temperature < 18) {
      Serial.printf("🌡️ Temperature: %.1f°C [LOW] - Sent\n", temperature);
    } else {
      Serial.printf("🌡️ Temperature: %.1f°C [NORMAL] - Sent\n", temperature);
    }
    
    lastTemperature = temperature;
    lastHumidity = humidity;
  }
}

// ==================== Send Sensor Data to Backend ====================
void sendSensorData(int sensorId, String value) {
  if (WiFi.status() != WL_CONNECTED) return;
  
  String url = String(serverUrl) + "/devices/" + String(sensorId) + "/sensor";
  
  http.begin(wifiClient, url);
  http.addHeader("Content-Type", "application/json");
  http.setTimeout(300);
  
  StaticJsonDocument<200> doc;
  doc["sensor_value"] = value;
  
  String jsonString;
  serializeJson(doc, jsonString);
  
  int httpCode = http.PUT(jsonString);
  
  // Silent on success, only print failures occasionally
  if (httpCode != 200 && httpCode != 201) {
    static int failCount = 0;
    failCount++;
    if (failCount % 10 == 0) {
      Serial.printf("❌ Sensor %d update failed: HTTP %d\n", sensorId, httpCode);
    }
  }
  
  http.end();
}
