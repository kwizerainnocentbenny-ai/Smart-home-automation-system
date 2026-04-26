# System Design

User interacts with a web dashboard to control devices.

Flow:
User → Dashboard → Backend API → ESP32 → Appliance

Communication:
- HTTP for control
- MQTT (optional extension)
