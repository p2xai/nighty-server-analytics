# Nighty Server Analytics & Dashboard

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)

> **A self-hosted Discord server analytics scraper and web dashboard for member tracking, snapshots, and trends.**

---

## Quickstart

1. **Clone this repository and enter the folder:**
   ```sh
   git clone https://github.com/p2xai/nighty-server-analytics.git
   cd nighty-server-analytics
   ```

2. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```
   Or install manually:
   ```sh
   pip install flask requests python-dotenv
   ```

3. **Copy/Drag `server analytics.py` to your Nighty /scripts folder**
   ```sh
   %appdata%\Nighty Selfbot\data\scripts
   ```

4. **Toggle the analytics script in Nighty**
   ```sh
   # In Discord run:
   <p>analytics api start
   ```

5. **Start the dashboard:**
   ```sh
   python analytics_dashboard.py
   ```

6. **Open your browser:**
   - Go to [http://127.0.0.1:5000/](http://127.0.0.1:5000/)

---

## Configuration (Optional)

You can create a `.env` file in the project root to customize settings. A sample configuration file `env.example` is provided - copy it to `.env` and customize as needed:

```env
# Database path (defaults to ./json/analytics_test.db if not set)
DB_PATH=/path/to/your/database.db

# API token for micro-API communication
NIGHTY_API_TOKEN=your_secret_token_here

# Dashboard URL for notifications
ANALYTICS_DASHBOARD_URL=http://127.0.0.1:5000

# Debug mode
DEBUG=True
```

> **Note:** If no `.env` file is present, the system will use default values and work exactly as before.

---

## Project Structure

```
nighty-server-analytics/
  README.md
  requirements.txt
  server analytics.py
  analytics_dashboard.py
  .env (optional)
  json/
    analytics_test.db (auto-created)
    global_analytics_webhook.json (auto-created)
```
> **Note:** The database and configuration files are auto-created in the `json/` directory by default.

---

## Features

- Landing page with 24-hour statistics (memberships, snapshots, servers)
- Per-server dashboards with charts, trends, and configuration
- Database search with server filter, user history, CSV/JSON export
- Server configuration editor (auto snapshot, retention, etc.)
- Analytics configuration (webhook, global settings)
- API endpoints for automation and integration
- SQLite backend for portability


---

## Dashboard Endpoints

- `/` or `/lander`: Landing page with 24-hour statistics
- `/dashboard?guild_id=...`: Per-server analytics dashboard
- `/database`: Search and export user/member data
- `/config`: Server configuration editor
- `/analytics-config`: Analytics/webhook configuration

### Key API routes
- `/api/24hr_stats`: 24-hour summary (memberships, snapshots, servers)
- `/api/servers`: List all tracked servers
- `/api/search_user`: Search users (with filters)
- `/api/search_user_all`: Download all search results
- `/api/server_configs`: Get all server configurations
- `/api/update_config`: Update server configuration
- `/api/take_snapshot/<guild_id>`: Manual snapshot
- `/api/fetch_members/<guild_id>`: Trigger member fetch
- `/api/user_history?member_id=...`: Get full history for a user

---

## Data Model

- **SQLite Database:** `analytics_test.db`
- **Tables:**
  - `snapshots`: Server state snapshots (member count, channels, etc.)
  - `demographics`: Member join/account data
  - `server_config`: Per-server configuration (auto snapshot, retention, etc.)

---


## License

MIT License (see code headers for details) 
