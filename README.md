# Nighty Server Analytics & Dashboard

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)

> **A self-hosted Discord server analytics scraper and web dashboard for member tracking, snapshots, and trends.**

---

## Quickstart

1. **Clone this repository and enter the folder:**
   ```sh
   git clone https://github.com/p2xai/nighty-server-analytics.git
   cd nighty-server-analytics/server_analytics
   ```
2. **Install dependencies:**
   ```sh
   pip install flask requests
   ```
3. **Start the analytics script in Nighty**
   ```sh
   python server_analytics.py
   # In Nighty/Discord run:
   <p>analytics api start
   ```
4. **Start the dashboard:**
   ```sh
   python analytics_dashboard.py
   ```
5. **Open your browser:**
   - Go to [http://localhost:5000/](http://localhost:5000/)

---

## Project Structure

```
server_analytics/
  README.md
  server_analytics.py
  analytics_dashboard.py
  analytics_test.db (auto-created)
  server_member_tracking/ (auto-created)
```

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

## Components

### server_analytics.py
- Collects and stores member snapshots, demographics, and server statistics in SQLite
- Supports periodic/manual snapshots, data retention, export, and migration
- Provides CLI and micro-API for automation

### analytics_dashboard.py
- Flask web application for viewing, searching, and managing analytics data
- Integrated dashboard, configuration, and search interface

---

## API Endpoints (Dashboard)

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

## Customization and Extending

- Add new analytics or export features by editing `server_analytics.py`
- Customize dashboard UI/UX in `analytics_dashboard.py` (HTML/JS is inline)
- Add new API endpoints as needed for automation or integration

---

## Troubleshooting

- **404 or 'Unexpected token <' errors:** Ensure all API endpoints are present and not commented out
- **Database errors:** Check that `analytics_test.db` is writable and not corrupted
- **Bot permissions:** The analytics engine must have permission to read members and channels
- **Webhooks:** Configure via `/analytics-config` for logging

---

## License

MIT License (see code headers for details) 
