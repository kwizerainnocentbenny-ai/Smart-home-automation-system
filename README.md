🏠 Smart Home IoT System
An IoT-based smart home automation system built using ESP32, FastAPI, and a web dashboard for real-time monitoring and control of home appliances.
________________________________________
🚀 Features
•	Remote control of home devices (ON/OFF)
•	Real-time interaction via web dashboard
•	Embedded system using ESP32
•	Backend API using FastAPI
•	Smart Scene based monitoring
•	Real-time Individual room control
•	Scalable architecture for IoT expansion
________________________________________
🧠 System Architecture
User →Web Dashboard →FastAPI Backend →ESP32 → Relay(when required) → Device
________________________________________
🛠️ Technologies Used
•	ESP32 (Embedded System)
•	FastAPI (Backend)
•	HTML,CSS, JavaScript (Frontend)
•	C, C++, Arduino languages (Firmware development)
•	MySQL / MariaDB (Database - optional)
•	HTTP / MQTT (Communication)
________________________________________
📂 Project Structure
•	firmware/ – ESP32 code
•	backend/FastAPI – API server
•	frontend/ – Web interface
•	documentation/ – System design documentation
________________________________________
▶️ How to Run
Backend
cd backend
pip install -r requirements.txt
uvicorn smart_home:app --reload
Dashboard
Open home_dashboard.html in your browser.
________________________________________
📸 Screenshots
(present in media folder)
    ________________________________________
🔧 Future Improvements
•	MQTT integration
•	Mobile app control
•	AI-based automation
________________________________________
👨💻 Author
KWIZERA Innocent

