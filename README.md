# JJM Automation Utility

A comprehensive FastAPI-based web application for automating the management of asset metadata, PI tags creation, and MQTT topic monitoring for the Jal Jeevan Mission (JJM) water supply automation project.

## 🚀 Overview

This utility automates the process of:
- **Asset Metadata Management**: Upload and validate Excel files containing water infrastructure asset data
- **PI Tags Creation**: Automatically create AVEVA PI tags for monitoring water infrastructure
- **MQTT Topic Monitoring**: Check communication status of IoT devices through MQTT broker
- **Data Validation**: Cross-reference asset metadata with MQTT topic availability
- **Status Management**: Update database records based on real-time device communication

## 🏗️ Architecture

### Core Components

- **FastAPI Backend**: RESTful API with async support
- **Database Layer**: SQL Server integration with pyodbc
- **PI System Integration**: AVEVA PI Server connectivity for tag management
- **MQTT Communication**: Real-time device monitoring via MQTT broker
- **Web Interface**: HTMX-powered responsive frontend with Tailwind CSS

### Key Features

- **Three-Step Processing Pipeline**:
  1. Asset metadata ingestion and database insertion
  2. Data validation against existing records
  3. PI tag creation and MQTT status verification

- **Real-time Monitoring**: WebSocket support for live logging (configurable)
- **Excel Processing**: Support for .xlsx and .xls file formats
- **Color-coded Reports**: Visual status indicators in generated Excel reports
- **Regional Configuration**: Multi-region support with specific PI Server mappings

## 📋 Prerequisites

### System Requirements
- Python 3.8+
- SQL Server (with ODBC Driver 17)
- AVEVA PI Server
- MQTT Broker access
- Windows environment (for PI SDK integration)

### Required Services
- SQL Server Database
- AVEVA PI Server (192.168.1.115 or configured)
- MQTT Broker (14.99.99.166:1889 or configured)

## 🛠️ Installation

### 1. Clone the Repository
```bash
git clone https://github.com/CereBulb/JJM-PI-Automation.git
cd JJM-PI-Automation
```

### 2. Create Virtual Environment
```bash
python -m venv venv
venv\Scripts\activate  # Windows
# or
source venv/bin/activate  # Linux/Mac
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Environment Configuration
Create `.env` file in the `app/` directory based on `app/.env-dev`:

```env
# Database Configuration
DB_DRIVER=ODBC Driver 17 for SQL Server
DB_SERVER=your_sql_server
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password

# PI Server Configuration
PI_SERVER=your_pi_server_ip
PI_SDK_REFRENCE=OSIsoft.AFSDK
PI_SYS_PATH=C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0\

# JSON File Paths (for topic-tag mappings)
PUNE_PRESSURE_JSON_FILE=path_to_pune_pressure.json
PRESSURE_JSON_FILE=path_to_pressure.json
PUNE_JSON_FILE=path_to_pune.json
TAGS_JSON_FILE=path_to_tags.json
```

### 5. Database Setup
Ensure your SQL Server has the required tables:
- `dbo.Asset_Metadata_New`
- `dbo.Asset_Metadata_Count`

## 🚀 Running the Application

### Development Mode
```bash
uvicorn app.main:app --reload
```

### Production Mode
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Access the application at `http://localhost:8000`

## 📖 Usage Guide

### Step 1: Asset Metadata Upload
1. Navigate to the web interface
2. Upload Excel file containing asset metadata
3. File must contain all required columns as defined in `SQL_COLS`
4. System validates and inserts new records into database

### Step 2: Validation File Upload
1. Upload Excel file with MQTT topic mappings
2. System cross-references with existing database records
3. Updates record status to 'Validated' for matching entries
4. Download validation status report

### Step 3: PI Tags Creation & MQTT Verification
1. System automatically triggers after Step 2
2. Creates PI tags for validated assets:
   - Chlorine level tags (`_CL`)
   - Flow rate tags (`_FL_RATE`, `_TOT_FL`)
   - Pressure tags (`_PRESS`)
   - Sensor error tags (`_SEN_ERR_CL`, `_SEN_ERR_FL_MTR`)
3. Checks MQTT communication status
4. Updates database records to 'Active' or 'Inactive' based on MQTT availability
5. Generates comprehensive status report

## 📊 File Formats

### Asset Metadata Excel Format
Required columns include:
- Site_name, State, Region, Circle, Division
- Scheme_ID, Scheme_Name, Village_Name
- Name_of_the_Reservoir, Name_of_Base_Reservoir
- Pump and reservoir details
- Geographic coordinates
- And 60+ other infrastructure fields

### MQTT Topics Excel Format
Required columns:
- Region, Circle, Division, Sub Division, Block
- Schme ID Name, Village, Reservoir
- Topic For CL, Topic For Flow Meter, Topic For Pressure
- CL Type

## 🔧 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main upload interface |
| `/ingest_asset_metadata` | POST | Upload asset metadata file |
| `/validate_metadata` | POST | Upload validation file |
| `/final_upload` | POST | Create PI tags and check MQTT |
| `/download_validation_status` | GET | Download validation report |
| `/download_active_tags` | GET | Download tags creation report |
| `/logs` | GET | View application logs |

## 🏷️ PI Tag Naming Convention

```
JJM.MH_JJM_{SCHEME_ID}_{VILLAGE}_RES_{RESERVOIR}_{TAG_TYPE}
```

**Tag Types:**
- `CL` - Chlorine level
- `FL_RATE` - Flow rate
- `TOT_FL` - Total flow
- `PRESS` - Pressure
- `SEN_ERR_CL` - Chlorine sensor error
- `SEN_ERR_FL_MTR` - Flow meter sensor error

## 🌐 MQTT Integration

### Broker Configuration
- **Host**: 14.99.99.166
- **Port**: 1889
- **Username**: MQTT
- **Password**: Mqtt@123
- **Timeout**: 60 seconds

### Topic Classification
- **Flow Meter Topics**: Classified based on `Flow_Error` field
- **Chlorine Topics**: Classified based on `Cl_Error` and `AI1` fields
- **Pressure Topics**: Classified based on data availability

## 📁 Project Structure

```
JJM-Automation-Utility/
├── app/
│   ├── main.py                 # FastAPI application entry point
│   ├── database.py             # Database connection management
│   ├── routers/
│   │   └── upload.py           # Upload and processing endpoints
│   ├── utils/
│   │   ├── mqtt_topics_utility.py    # MQTT communication
│   │   ├── pi_tag_utility.py         # PI tag creation
│   │   ├── topic_tags_json_utility.py # JSON mapping utilities
│   │   ├── utils.py                  # Helper functions
│   │   ├── logger.py                 # Logging configuration
│   │   └── websocket_manager.py      # WebSocket management
│   ├── templates/
│   │   ├── base.html           # Base HTML template
│   │   ├── upload.html         # Main upload interface
│   │   └── terminal_logs.html  # Log viewer component
│   ├── static/
│   │   ├── style.css           # Custom styles
│   │   └── cb-logo-tagline-lightbg.png
│   └── .env-dev                # Environment configuration
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

## 🔍 Monitoring & Logging

- Application logs are configured via `logger.py`
- WebSocket support for real-time log streaming
- Comprehensive error handling and reporting
- Status tracking throughout the processing pipeline

## 🛡️ Error Handling

- Database transaction rollback on errors
- MQTT connection timeout handling
- PI Server connection error management
- File format validation
- Comprehensive error reporting in UI

## 🔧 Configuration

### Regional PI Server Mapping
```python
REGION_POINT_SOURCE_MAP = {
    "amravati": "AU",
    "nagpur": "NA",
    "chhatrapati sambhajinagar": "CS",
    "nashik": "NASHIK",
    "pune": "PUNE",
    "konkan": "KONKAN",
}
```

### MQTT Topic Types
- `fm` - Flow meter topics
- `cl` - Chlorine level topics  
- `pressure` - Pressure monitoring topics

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📝 License

This project is developed for the Jal Jeevan Mission water supply automation initiative.

## 📞 Support

For issues and questions, please contact the development team or create an issue in the repository.

---

**Note**: This application requires proper configuration of SQL Server, AVEVA PI Server, and MQTT broker connectivity for full functionality.