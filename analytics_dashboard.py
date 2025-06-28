from flask import Flask, jsonify, render_template_string, request, g
import sqlite3
import os
import json
from datetime import datetime, timezone
from collections import defaultdict, Counter
import requests
import time
import re

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), "json", "analytics_test.db")

# Configuration for NightyScript micro-API
NIGHTY_API_BASE_URL = os.environ.get('NIGHTY_API_BASE_URL', 'http://127.0.0.1:5500')

WEBHOOK_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'json', 'global_analytics_webhook.json')

def get_db():
    if 'db_conn' not in g:
        g.db_conn = sqlite3.connect(DB_PATH)
        g.db_conn.row_factory = sqlite3.Row
    return g.db_conn

@app.teardown_appcontext
def close_db(exception=None):
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()

def validate_and_repair_database():
    """Comprehensive database validation and repair function"""
    print(" Validating database schema...")
    
    # Ensure the database directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    db = get_db()
    cursor = db.cursor()
    
    # Define expected schema
    expected_schema = {
        'server_config': {
            'columns': [
                ('guild_id', 'TEXT', 'PRIMARY KEY'),
                ('guild_name', 'TEXT', ''),
                ('auto_snapshot', 'BOOLEAN', 'DEFAULT 0'),
                ('last_auto_snapshot', 'TEXT', ''),
                ('first_snapshot_date', 'TEXT', ''),
                ('chart_style', 'TEXT', 'DEFAULT \'emoji\''),
                ('snapshot_retention_days', 'INTEGER', 'DEFAULT 90'),
                ('auto_snapshot_interval_hours', 'INTEGER', 'DEFAULT 20'),
                ('last_snapshot', 'TEXT', '')
            ]
        },
        'snapshots': {
            'columns': [
                ('id', 'INTEGER', 'PRIMARY KEY AUTOINCREMENT'),
                ('guild_id', 'TEXT', 'NOT NULL'),
                ('guild_name', 'TEXT', ''),
                ('timestamp', 'TEXT', 'NOT NULL'),
                ('member_count', 'INTEGER', ''),
                ('channel_count', 'INTEGER', ''),
                ('text_channels', 'INTEGER', ''),
                ('voice_channels', 'INTEGER', ''),
                ('categories', 'INTEGER', ''),
                ('role_count', 'INTEGER', ''),
                ('bots', 'INTEGER', ''),
                ('boosters', 'INTEGER', ''),
                ('is_auto', 'BOOLEAN', 'DEFAULT 0')
            ]
        },
        'demographics': {
            'columns': [
                ('guild_id', 'TEXT', 'NOT NULL'),
                ('member_id', 'TEXT', 'NOT NULL'),
                ('name', 'TEXT', ''),
                ('account_created', 'TEXT', ''),
                ('joined_at', 'TEXT', ''),
                ('timestamp', 'TEXT', ''),
                ('PRIMARY KEY', '(guild_id, member_id)', '')
            ]
        },
        'demographics_servers': {
            'columns': [
                ('guild_id', 'TEXT', 'PRIMARY KEY')
            ]
        }
    }
    
    issues_found = []
    fixes_applied = []
    
    # Check if all tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = [row[0] for row in cursor.fetchall()]
    
    for table_name, table_info in expected_schema.items():
        if table_name not in existing_tables:
            issues_found.append(f"Missing table: {table_name}")
            # Create the missing table
            columns_def = []
            for col_name, col_type, col_constraints in table_info['columns']:
                if col_name == 'PRIMARY KEY':
                    columns_def.append(f"PRIMARY KEY {col_type}")
                else:
                    column_def = f"{col_name} {col_type}"
                    if col_constraints:
                        column_def += f" {col_constraints}"
                    columns_def.append(column_def)
            
            create_sql = f"CREATE TABLE {table_name} ({', '.join(columns_def)})"
            try:
                cursor.execute(create_sql)
                fixes_applied.append(f"Created table: {table_name}")
            except Exception as e:
                issues_found.append(f"Failed to create table {table_name}: {e}")
    
    # Check columns in each table
    for table_name, table_info in expected_schema.items():
        if table_name in existing_tables:
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = {row[1]: row[2] for row in cursor.fetchall()}
            
            for col_name, col_type, col_constraints in table_info['columns']:
                if col_name == 'PRIMARY KEY':
                    continue  # Skip primary key constraints for now
                
                if col_name not in existing_columns:
                    issues_found.append(f"Missing column: {table_name}.{col_name}")
                    # Add the missing column
                    try:
                        add_column_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
                        if col_constraints and 'DEFAULT' in col_constraints:
                            add_column_sql += f" {col_constraints}"
                        cursor.execute(add_column_sql)
                        fixes_applied.append(f"Added column: {table_name}.{col_name}")
                    except Exception as e:
                        issues_found.append(f"Failed to add column {table_name}.{col_name}: {e}")
    
    # Check for any foreign key constraints that might be missing
    # (This is a simplified check - SQLite doesn't enforce foreign keys by default)
    
    # Commit any changes
    if fixes_applied:
        db.commit()
        print(f" Applied {len(fixes_applied)} fixes:")
        for fix in fixes_applied:
            print(f"   - {fix}")
    
    if issues_found:
        print(f"  Found {len(issues_found)} issues:")
        for issue in issues_found:
            print(f"   - {issue}")
    else:
        print(" Database schema is valid and complete!")
    
    return len(issues_found) == 0

def init_database():
    """Initialize the database with the required schema"""
    # Use the new validation and repair function
    validate_and_repair_database()

def get_global_webhook_url():
    # For now, use a global webhook config file (can be per-server later)
    if os.path.exists(WEBHOOK_CONFIG_PATH):
        try:
            with open(WEBHOOK_CONFIG_PATH, 'r') as f:
                data = json.load(f)
                return data.get('webhook_url')
        except Exception:
            return None
    return None

def set_global_webhook_url(url):
    os.makedirs(os.path.dirname(WEBHOOK_CONFIG_PATH), exist_ok=True)  # Ensure directory exists
    with open(WEBHOOK_CONFIG_PATH, 'w') as f:
        json.dump({'webhook_url': url}, f)

@app.route('/')
def root_lander():
    return lander_page()

@app.route('/lander')
def lander_page():
    return render_template_string(r'''
<!DOCTYPE html>
<html>
<head>
    <title>Server Analytics Lander</title>
    <style>
        body { display: flex; margin: 0; font-family: 'Segoe UI', Arial, sans-serif; background: #181a1b; }
        #sidebar {
            width: 220px;
            background: #23272a;
            color: #e0e0e0;
            height: 100vh;
            position: fixed;
            left: 0; top: 0; bottom: 0;
            overflow-y: auto;
            overflow-x: hidden;
            transition: width 0.2s;
            z-index: 10;
            font-family: inherit;
            scrollbar-width: none;
        }
        #sidebar::-webkit-scrollbar { display: none; }
        #sidebar.collapsed {
            width: 40px;
            min-width: 40px;
        }
        #sidebar .toggle-btn {
            background: none; border: none; color: #90caf9; font-size: 1.5em; width: 100%; text-align: left; padding: 8px;
            cursor: pointer;
            outline: none;
        }
        #sidebar .toggle-btn {
            text-align: center;
            font-size: 2em;
            padding: 8px 0;
        }
        #sidebar #homeIcon {
            display: block;
            text-align: center;
            font-size: 2em;
            padding: 10px 0 6px 0;
            color: #90caf9;
            text-decoration: none;
            margin-bottom: 2px;
        }
        #sidebar.collapsed #homeIcon svg {
            margin: 0 auto;
        }
        #sidebar.collapsed #serverList,
        #sidebar.collapsed #databaseLink,
        #sidebar.collapsed #configLink,
        #sidebar.collapsed #analyticsConfigLink {
            display: none !important;
        }
        #sidebar.collapsed #serverList li,
        #sidebar.collapsed #databaseLink,
        #sidebar.collapsed #configLink,
        #sidebar.collapsed #analyticsConfigLink {
            pointer-events: none;
        }
        #serverList { list-style: none; padding: 0; margin: 0; }
        #serverList li {
            padding: 12px 16px;
            cursor: pointer;
            border-bottom: 1px solid #333;
            transition: background 0.2s, color 0.2s;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            font-family: inherit;
        }
        #serverList li.active, #serverList li:hover { background: #181a1b; color: #90caf9; }
        #main-content {
            margin-left: 220px;
            padding: 40px 32px;
            width: 100%;
            min-height: 100vh;
            background: #181a1b;
        }
        #sidebar.collapsed + #main-content { margin-left: 40px; }
        .lander-row {
            display: flex;
            flex-wrap: wrap;
            gap: 32px;
            align-items: flex-start;
            margin-top: 32px;
        }
        .summary-widget {
            background: #23272a;
            color: #e0e0e0;
            border-radius: 12px;
            box-shadow: 0 2px 12px #000a;
            padding: 28px 36px 24px 36px;
            min-width: 320px;
            max-width: 400px;
            flex: 0 0 340px;
        }
        .member-chart-widget {
            background: #23272a;
            color: #e0e0e0;
            border-radius: 12px;
            box-shadow: 0 2px 12px #000a;
            padding: 28px 36px 24px 36px;
            min-width: 320px;
            max-width: 600px;
            flex: 1 1 400px;
            display: flex;
            flex-direction: column;
            align-items: stretch;
            height: 100%;
        }
        .member-chart-widget canvas {
            width: 100% !important;
            height: 260px !important;
            max-width: 100%;
            display: block;
        }
        @media (max-width: 900px) {
            .lander-row { flex-direction: column; gap: 0; }
            .summary-widget, .member-chart-widget { max-width: 100%; min-width: unset; margin: 0 0 24px 0; }
        }
        @media (max-width: 600px) {
            .summary-widget, .member-chart-widget { margin-left: 8px; margin-right: 8px; min-width: unset; max-width: unset; }
            #main-content { padding: 16px 4px; }
        }
        .summary-title {
            color: #90caf9;
            font-size: 1.25em;
            font-weight: bold;
            margin-bottom: 18px;
        }
        .summary-list {
            list-style: none;
            padding: 0;
            margin: 0;
        }
        .summary-list li {
            display: flex;
            align-items: center;
            margin-bottom: 14px;
            font-size: 1.08em;
        }
        .summary-label {
            flex: 1;
            color: #90caf9;
            font-weight: 500;
        }
        .summary-value {
            font-weight: bold;
            font-size: 1.1em;
            margin-left: 10px;
        }
        .summary-delta {
            margin-left: 12px;
            font-size: 0.98em;
            font-weight: 500;
        }
        .summary-delta.positive { color: #4caf50; }
        .summary-delta.negative { color: #f44336; }
        .summary-delta.zero { color: #aaa; }
    </style>
</head>
<body>
    <div id="sidebar">
        <div style="display:flex;flex-direction:column;align-items:center;gap:4px;padding:16px 0 8px 0;">
            <a href="/" id="homeIcon" aria-label="Home" style="display:flex;justify-content:center;align-items:center;font-size:2em;color:#90caf9;text-decoration:none;margin:0 0 8px 0;">
                <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#90caf9" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9.5L12 4l9 5.5V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V9.5z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
            </a>
            <button class="toggle-btn" onclick="toggleSidebar()" style="font-size:2em;padding:0;background:none;border:none;color:#90caf9;display:flex;justify-content:center;align-items:center;"></button>
        </div>
        <a href="/database" id="databaseLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:12px 16px 8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Database Search</a>
        <a href="/config" id="configLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Server Config</a>
        <ul id="serverList" style="padding-left:0;"></ul>
        <div style="margin-top: auto; padding-top: 20px; border-top: 1px solid #333;">
            <a href="/analytics-config" id="analyticsConfigLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Analytics Config</a>
        </div>
    </div>
    <div id="main-content">
        <div class="lander-row">
            <div class="summary-widget">
                <div class="summary-title">Last 24 Hours</div>
                <ul class="summary-list">
                    <li><span class="summary-label">Tracked Members</span> <span class="summary-value" id="membersTotal">...</span> <span class="summary-delta" id="membersDelta"></span></li>
                    <li><span class="summary-label">Snapshots</span> <span class="summary-value" id="snapshotsTotal">...</span> <span class="summary-delta" id="snapshotsDelta"></span></li>
                    <li><span class="summary-label">Tracked Servers</span> <span class="summary-value" id="serversTotal">...</span> <span class="summary-delta" id="serversDelta"></span></li>
                </ul>
            </div>
            <div class="member-chart-widget">
                <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
                    <div class="summary-title" style="margin-bottom:0;">Total Member Count</div>
                    <div>
                        <label for="memberCountRange" style="color:#90caf9;font-size:0.98em;margin-right:6px;">Range</label>
                        <select id="memberCountRange" style="background:#23272a;color:#e0e0e0;border:1px solid #333;border-radius:4px;padding:4px 8px;">
                            <option value="1">1d</option>
                            <option value="7" selected>7d</option>
                            <option value="30">30d</option>
                        </select>
                    </div>
                </div>
                <canvas id="totalMemberCountChart"></canvas>
            </div>
        </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
        function toggleSidebar() {
            document.getElementById('sidebar').classList.toggle('collapsed');
            document.getElementById('main-content').classList.toggle('collapsed');
        }
        async function loadSidebarServers() {
            const res = await fetch('/api/servers');
            const servers = await res.json();
            const list = document.getElementById('serverList');
            list.innerHTML = '';
            let lastSelected = localStorage.getItem('nighty_last_server');
            let found = false;
            servers.forEach((srv, idx) => {
                const li = document.createElement('li');
                li.innerHTML = `<span class='server-name'>${srv.name || srv.id}</span>`;
                li.onclick = () => {
                    localStorage.setItem('nighty_last_server', srv.id);
                    window.location.href = '/dashboard?guild_id=' + encodeURIComponent(srv.id);
                };
                list.appendChild(li);
                if ((lastSelected && srv.id === lastSelected) || (!lastSelected && idx === 0)) {
                    li.classList.add('active');
                    found = true;
                }
            });
            if (!found && servers.length > 0) {
                list.firstChild.classList.add('active');
            }
        }
        loadSidebarServers();
        async function load24hrStats() {
            try {
                const res = await fetch('/api/24hr_stats');
                const data = await res.json();
                document.getElementById('membersTotal').textContent = data.members.total.toLocaleString();
                setDelta('membersDelta', data.members.delta);
                document.getElementById('snapshotsTotal').textContent = data.snapshots.total.toLocaleString();
                setDelta('snapshotsDelta', data.snapshots.delta);
                document.getElementById('serversTotal').textContent = data.servers.total.toLocaleString();
                setDelta('serversDelta', data.servers.delta);
            } catch (e) {
                document.getElementById('membersTotal').textContent = '...';
                document.getElementById('snapshotsTotal').textContent = '...';
                document.getElementById('serversTotal').textContent = '...';
            }
        }
        function setDelta(id, delta) {
            const el = document.getElementById(id);
            if (delta > 0) {
                el.textContent = `(+${delta.toLocaleString()})`;
                el.className = 'summary-delta positive';
            } else if (delta < 0) {
                el.textContent = `(${delta.toLocaleString()})`;
                el.className = 'summary-delta negative';
            } else {
                el.textContent = '(0)';
                el.className = 'summary-delta zero';
            }
        }
        load24hrStats();
        document.addEventListener('DOMContentLoaded', function() {
            let totalMemberCountChart = null;
            async function loadTotalMemberCountChart(days = 7) {
                let url, labelKey, dataKey;
                if (days == 1) {
                    url = '/api/tracked_members_over_time_hourly';
                    labelKey = 'hours';
                    dataKey = 'counts';
                } else {
                    url = '/api/tracked_members_over_time?days=' + days;
                    labelKey = 'dates';
                    dataKey = 'counts';
                }
                const res = await fetch(url);
                const data = await res.json();
                const ctx = document.getElementById('totalMemberCountChart').getContext('2d');
                if (totalMemberCountChart) totalMemberCountChart.destroy();
                // Format labels: for 1d, show only hour (HH:mm); for others, keep as is
                let labels = data[labelKey];
                if (days == 1) {
                    labels = labels.map(ts => ts.slice(11, 16)); // 'HH:mm'
                }
                totalMemberCountChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: 'Tracked Members',
                            data: data[dataKey],
                            borderColor: '#90caf9',
                            backgroundColor: 'rgba(144,202,249,0.1)',
                            fill: true,
                            tension: 0.3
                        }]
                    },
                    options: {
                        plugins: { legend: { display: false }, tooltip: { enabled: true } },
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            x: { title: { display: true, text: days == 1 ? 'Hour' : 'Date', color: '#90caf9' }, ticks: { color: '#e0e0e0' }, grid: { color: '#333' } },
                            y: {
                                title: { display: true, text: 'Tracked Members', color: '#90caf9' },
                                ticks: {
                                    color: '#e0e0e0',
                                    stepSize: 1,
                                    callback: function(value) { return Number(value).toLocaleString(); },
                                    precision: 0
                                },
                                grid: { color: '#333' },
                                beginAtZero: true
                            }
                        },
                        interaction: { mode: 'nearest', intersect: false },
                        hover: { mode: 'nearest', intersect: false }
                    }
                });
            }
            document.getElementById('memberCountRange').addEventListener('change', function() {
                loadTotalMemberCountChart(this.value);
            });
            // Initial load: use the dropdown's current value
            const initialRange = document.getElementById('memberCountRange').value;
            loadTotalMemberCountChart(initialRange);
        });
    </script>
</body>
</html>
''')

@app.route('/api/total_snapshots')
def total_snapshots():
    db = get_db()
    guild_id = request.args.get('guild_id')
    if guild_id:
        count = db.execute("SELECT COUNT(*) as count FROM snapshots WHERE guild_id = ?", (guild_id,)).fetchone()
    else:
        count = db.execute("SELECT COUNT(*) as count FROM snapshots").fetchone()
    return jsonify({'count': count['count']})

@app.route('/api/snapshots_24h')
def snapshots_24h():
    db = get_db()
    import datetime
    guild_id = request.args.get('guild_id')
    now = datetime.datetime.now(datetime.timezone.utc)
    hours = [(now - datetime.timedelta(hours=i)).replace(minute=0, second=0, microsecond=0) for i in range(23, -1, -1)]
    hour_labels = [h.strftime('%H:00') for h in hours]
    counts = []
    for h in hours:
        next_h = h + datetime.timedelta(hours=1)
        if guild_id:
            c = db.execute("SELECT COUNT(*) FROM snapshots WHERE guild_id = ? AND timestamp >= ? AND timestamp < ?", (guild_id, h.isoformat(), next_h.isoformat())).fetchone()[0]
        else:
            c = db.execute("SELECT COUNT(*) FROM snapshots WHERE timestamp >= ? AND timestamp < ?", (h.isoformat(), next_h.isoformat())).fetchone()[0]
        counts.append(c)
    return jsonify({'hours': hour_labels, 'counts': counts})

@app.route('/api/members_over_time')
def members_over_time():
    db = get_db()
    import datetime
    days = request.args.get('days', default=None, type=int)
    guild_id = request.args.get('guild_id')
    # Use snapshot member_count per day (last snapshot of each day)
    if guild_id:
        rows = db.execute("SELECT timestamp, member_count FROM snapshots WHERE guild_id = ? ORDER BY timestamp", (guild_id,)).fetchall()
    else:
        rows = db.execute("SELECT timestamp, member_count FROM snapshots ORDER BY timestamp", ()).fetchall()
    # Group by day, take the last snapshot of each day
    from collections import defaultdict
    day_map = defaultdict(list)
    for row in rows:
        day = row['timestamp'][:10]
        day_map[day].append((row['timestamp'], row['member_count']))
    all_days = sorted(day_map.keys())
    date_list = all_days
    if days is not None:
        today = datetime.date.today()
        date_list = [(today - datetime.timedelta(days=i)).isoformat() for i in range(days-1, -1, -1)]
    result_dates = []
    result_counts = []
    prev = 0
    for d in date_list:
        if d in day_map:
            # Use the last snapshot of the day
            count = day_map[d][-1][1]
            prev = count
        else:
            count = prev
        result_dates.append(d)
        result_counts.append(count)
    return jsonify({'dates': result_dates, 'counts': result_counts})

@app.route('/api/user_count')
def user_count():
    db = get_db()
    guild_id = request.args.get('guild_id')
    if guild_id:
        count = db.execute("SELECT COUNT(DISTINCT member_id) as count FROM demographics WHERE guild_id = ?", (guild_id,)).fetchone()
    else:
        count = db.execute("SELECT COUNT(DISTINCT member_id) as count FROM demographics").fetchone()
    return jsonify({'count': count['count']})

@app.route('/api/membership_count')
def membership_count():
    db = get_db()
    guild_id = request.args.get('guild_id')
    if guild_id:
        count = db.execute("SELECT COUNT(*) as count FROM demographics WHERE guild_id = ?", (guild_id,)).fetchone()
    else:
        count = db.execute("SELECT COUNT(*) as count FROM demographics").fetchone()
    return jsonify({'count': count['count']})

@app.route('/api/servers')
def list_servers():
    db = get_db()
    # For each unique guild_id, get the most recent snapshot's guild_name
    servers = db.execute('''
        SELECT s.guild_id, s.guild_name
        FROM snapshots s
        INNER JOIN (
            SELECT guild_id, MAX(timestamp) as max_ts
            FROM snapshots
            GROUP BY guild_id
        ) latest
        ON s.guild_id = latest.guild_id AND s.timestamp = latest.max_ts
    ''').fetchall()
    return jsonify([{'id': row['guild_id'], 'name': row['guild_name'] or str(row['guild_id'])} for row in servers])

@app.route('/api/server/<guild_id>/snapshots')
def server_snapshots(guild_id):
    db = get_db()
    group = request.args.get('group', 'snapshot')
    rows = db.execute(
        'SELECT timestamp, member_count FROM snapshots WHERE guild_id=? ORDER BY timestamp',
        (guild_id,)
    ).fetchall()
    import datetime
    from collections import defaultdict
    if group == 'snapshot':
        result = [
            {'timestamp': row['timestamp'], 'member_count': row['member_count']} for row in rows
        ]
    elif group == 'day':
        day_map = defaultdict(list)
        for row in rows:
            day = row['timestamp'][:10]
            day_map[day].append(row['member_count'])
        result = []
        prev = None
        for day in sorted(day_map.keys()):
            count = day_map[day][-1]  # last snapshot of the day
            result.append({'timestamp': day, 'member_count': count})
    elif group == 'week':
        week_map = defaultdict(list)
        for row in rows:
            dt = datetime.datetime.fromisoformat(row['timestamp'])
            year, week, _ = dt.isocalendar()
            key = f'{year}-W{week:02d}'
            week_map[key].append(row['member_count'])
        result = []
        for week in sorted(week_map.keys()):
            count = week_map[week][-1]  # last snapshot of the week
            result.append({'timestamp': week, 'member_count': count})
    else:
        result = [
            {'timestamp': row['timestamp'], 'member_count': row['member_count']} for row in rows
        ]
    return jsonify(result)

@app.route('/api/server/<guild_id>/demographics')
def server_demographics(guild_id):
    db = get_db()
    rows = db.execute(
        'SELECT member_id, name, account_created, joined_at FROM demographics WHERE guild_id=?',
        (guild_id,)
    ).fetchall()
    def format_utc(dtstr):
        if not dtstr:
            return ''
        try:
            dt = datetime.fromisoformat(dtstr.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M UTC')
        except Exception:
            return dtstr
    members = [
        {
            'member_id': row['member_id'],
            'name': row['name'],
            'account_created_raw': row['account_created'],
            'joined_at_raw': row['joined_at'],
        }
        for row in rows
    ]
    # Filter out nulls for sorting
    acc_sorted = [m for m in members if m['account_created_raw']]
    join_sorted = [m for m in members if m['joined_at_raw']]
    oldest_acc = sorted(acc_sorted, key=lambda m: m['account_created_raw'])[:3]
    newest_acc = sorted(acc_sorted, key=lambda m: m['account_created_raw'], reverse=True)[:3]
    longest_mem = sorted(join_sorted, key=lambda m: m['joined_at_raw'])[:3]
    newest_mem = sorted(join_sorted, key=lambda m: m['joined_at_raw'], reverse=True)[:3]
    def display_list(lst, key):
        return [
            {
                'member_id': m['member_id'],
                'name': m['name'],
                key: format_utc(m[key + '_raw'])
            }
            for m in lst
        ]
    return jsonify({
        'total': len(members),
        'oldest_accounts': display_list(oldest_acc, 'account_created'),
        'newest_accounts': display_list(newest_acc, 'account_created'),
        'longest_members': display_list(longest_mem, 'joined_at'),
        'newest_members': display_list(newest_mem, 'joined_at')
    })

@app.route('/database')
def database_page():
    return render_template_string(r"""
    <html>
    <head>
        <title>Database User Search</title>
        <style>
            body { display: flex; margin: 0; font-family: 'Segoe UI', Arial, sans-serif; background: #181a1b; }
            #sidebar {
                width: 220px;
                background: #23272a;
                color: #e0e0e0;
                height: 100vh;
                position: fixed;
                left: 0; top: 0; bottom: 0;
                overflow-y: auto;
                overflow-x: hidden;
                transition: width 0.2s;
                z-index: 10;
                font-family: inherit;
                scrollbar-width: none;
            }
            #sidebar::-webkit-scrollbar { display: none; }
            #sidebar.collapsed {
                width: 40px;
                min-width: 40px;
            }
            #sidebar.collapsed .toggle-btn {
                text-align: center;
                font-size: 2em;
                padding: 8px 0;
            }
            #sidebar.collapsed #serverList, #sidebar.collapsed #databaseLink, #sidebar.collapsed #configLink, #sidebar.collapsed #analyticsConfigLink {
                display: none;
            }
            #sidebar .toggle-btn {
                background: none; border: none; color: #90caf9; font-size: 1.5em; width: 100%; text-align: left; padding: 8px;
                cursor: pointer;
                outline: none;
            }
            #serverList { list-style: none; padding: 0; margin: 0; }
            #serverList li {
                padding: 12px 16px;
                cursor: pointer;
                border-bottom: 1px solid #333;
                transition: background 0.2s, color 0.2s;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                font-family: inherit;
            }
            #serverList li.active, #serverList li:hover { background: #181a1b; color: #90caf9; }
            #main-content {
                margin-left: 220px;
                padding: 24px;
                width: 100%;
                transition: margin-left 0.2s;
            }
            #sidebar.collapsed + #main-content { margin-left: 40px; }
            #container { display: flex; flex-direction: row; align-items: flex-start; padding: 32px 0 0 0; }
            #search-col {
                min-width: 320px;
                max-width: 400px;
                padding: 0 0 0 32px;
            }
            #searchBox {
                padding: 10px;
                width: 100%;
                border-radius: 4px;
                border: 1px solid #333;
                background: #23272a;
                color: #e0e0e0;
                font-size: 1.1em;
                margin-bottom: 12px;
            }
            #results-col {
                flex: 1;
                margin-left: 48px;
            }
            #results-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 16px;
            }
            #downloadBtn {
                background: #90caf9;
                color: #181a1b;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                cursor: pointer;
                font-weight: bold;
                font-size: 0.9em;
            }
            #downloadBtn:hover {
                background: #64b5f6;
            }
            #downloadBtn:disabled {
                background: #666;
                cursor: not-allowed;
            }
            #results-box {
                background: #23272a;
                color: #e0e0e0;
                border-radius: 8px;
                padding: 24px;
                min-height: 120px;
                box-shadow: 0 2px 8px #000a;
                margin-top: 0;
                max-height: 70vh;
                overflow-y: auto;
            }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #333; padding: 8px; }
            th { background: #181a1b; color: #90caf9; position: sticky; top: -24px; z-index: 1; }
            tr:nth-child(even) { background: #23272a; }
            tr:nth-child(odd) { background: #202225; }
            .loading {
                text-align: center;
                padding: 20px;
                color: #90caf9;
            }
            .loading::after {
                content: '';
                display: inline-block;
                width: 20px;
                height: 20px;
                border: 3px solid #90caf9;
                border-radius: 50%;
                border-top-color: transparent;
                animation: spin 1s ease-in-out infinite;
                margin-left: 10px;
            }
            @keyframes spin {
                to { transform: rotate(360deg); }
            }
            .search-stats {
                background: #181a1b;
                color: #e0e0e0;
                padding: 8px 12px;
                border-radius: 4px;
                margin-bottom: 12px;
                font-size: 0.9em;
            }
            .load-more-btn {
                background: #90caf9;
                color: #181a1b;
                border: none;
                padding: 10px 20px;
                border-radius: 4px;
                cursor: pointer;
                font-weight: bold;
                margin: 16px auto;
                display: block;
            }
            .load-more-btn:hover {
                background: #64b5f6;
            }
            .load-more-btn:disabled {
                background: #666;
                cursor: not-allowed;
            }
            .search-input-container {
                position: relative;
            }
            .search-spinner {
                position: absolute;
                right: 10px;
                top: 50%;
                transform: translateY(-50%);
                display: none;
            }
            .search-spinner.active {
                display: block;
            }
        </style>
    </head>
    <body>
        <div id="sidebar">
            <div style="display:flex;flex-direction:column;align-items:center;gap:4px;padding:16px 0 8px 0;">
                <a href="/" id="homeIcon" aria-label="Home" style="display:flex;justify-content:center;align-items:center;font-size:2em;color:#90caf9;text-decoration:none;margin:0 0 8px 0;">
                    <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#90caf9" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9.5L12 4l9 5.5V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V9.5z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
                </a>
                <button class="toggle-btn" onclick="toggleSidebar()" style="font-size:2em;padding:0;background:none;border:none;color:#90caf9;display:flex;justify-content:center;align-items:center;"></button>
            </div>
            <a href="/database" id="databaseLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:12px 16px 8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Database Search</a>
            <a href="/config" id="configLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Server Config</a>
            <ul id="serverList"></ul>
            <div style="margin-top: auto; padding-top: 20px; border-top: 1px solid #333;">
                <a href="/analytics-config" id="analyticsConfigLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Analytics Config</a>
            </div>
        </div>
        <div id="main-content">
            <div id="container">
                <div id="search-col">
                    <h2 style="margin-top:0; color:#90caf9;">User Search</h2>
                    <div id="userCountBox" style="background:#23272a;color:#e0e0e0;border-radius:8px;padding:16px;margin-bottom:16px;box-shadow:0 2px 8px #000a;">
                        <div style="color:#90caf9;font-weight:bold;margin-bottom:4px;">Total Users Tracked</div>
                        <div id="userCount" style="font-size:1.5em;font-weight:bold;">Loading...</div>
                    </div>
                    <label for="serverFilter" style="color:#90caf9;font-weight:bold;">Server:</label>
                    <select id="serverFilter" style="width:100%;margin-bottom:12px;padding:8px;border-radius:4px;background:#23272a;color:#e0e0e0;border:1px solid #333;font-size:1.1em;"></select>
                    <div class="search-input-container">
                        <input id="searchBox" type="text" placeholder="Enter username..." autocomplete="off" />
                        <div class="search-spinner" id="searchSpinner">
                            <div style="width:16px;height:16px;border:2px solid #90caf9;border-radius:50%;border-top-color:transparent;animation:spin 1s ease-in-out infinite;"></div>
                        </div>
                    </div>
                </div>
                <div id="results-col">
                    <div id="results-header">
                        <h3>Search Results</h3>
                        <div>
                            <button id="downloadBtn" onclick="downloadCSV()">Download CSV</button>
                            <button id="downloadJsonBtn" onclick="downloadJSON()" style="margin-left:8px;">Download JSON</button>
                        </div>
                    </div>
                    <div id="searchStats" class="search-stats" style="display:none;"></div>
                    <div id="results-box">
                        <div id="results"><span style='color:#888'>Search results will appear here.</span></div>
                    </div>
                </div>
            </div>
            <!-- User history modal -->
            <div id="userHistoryModal" style="display:none;position:fixed;top:0;left:0;width:100vw;height:100vh;background:rgba(24,26,27,0.92);z-index:1000;align-items:center;justify-content:center;">
                <div style="background:#23272a;padding:32px 36px 24px 36px;border-radius:12px;max-width:600px;width:90vw;max-height:80vh;overflow-y:auto;box-shadow:0 2px 16px #000a;position:relative;">
                    <button onclick="closeUserHistory()" style="position:absolute;top:12px;right:16px;background:none;border:none;color:#90caf9;font-size:1.5em;cursor:pointer;">&times;</button>
                    <h3 id="userHistoryTitle" style="margin-top:0;color:#90caf9;"></h3>
                    <div id="userHistoryContent" style="margin-top:12px;"></div>
                </div>
            </div>
        </div>
        <script>
            let lastQuery = '';
            let currentSearchResults = [];
            let currentOffset = 0;
            let currentLimit = 250;
            let hasMoreResults = false;
            let isLoading = false;
            let searchTimeout = null;
            let selectedServer = '';

            function toggleSidebar() {
                document.getElementById('sidebar').classList.toggle('collapsed');
                document.getElementById('main-content').classList.toggle('collapsed');
            }

            async function loadServers() {
                const res = await fetch('/api/servers');
                const servers = await res.json();
                const list = document.getElementById('serverList');
                list.innerHTML = '';
                let lastSelected = localStorage.getItem('nighty_last_server');
                let found = false;
                servers.forEach((srv, idx) => {
                    const li = document.createElement('li');
                    li.innerHTML = `<span class='server-name'>${srv.name || srv.id}</span>`;
                    li.onclick = () => {
                        localStorage.setItem('nighty_last_server', srv.id);
                        window.location.href = '/dashboard?guild_id=' + encodeURIComponent(srv.id);
                    };
                    list.appendChild(li);
                    if ((lastSelected && srv.id === lastSelected) || (!lastSelected && idx === 0)) {
                        li.classList.add('active');
                        found = true;
                    }
                });
                // Populate server dropdown filter
                const filter = document.getElementById('serverFilter');
                filter.innerHTML = `<option value="">All Servers</option>`;
                servers.forEach(srv => {
                    const opt = document.createElement('option');
                    opt.value = srv.id;
                    opt.textContent = srv.name || srv.id;
                    filter.appendChild(opt);
                });
                filter.value = selectedServer;
            }

            document.addEventListener('DOMContentLoaded', () => {
                loadServers();
                loadUserCount();
                document.getElementById('searchBox').addEventListener('input', onSearchInput);
                document.getElementById('serverFilter').addEventListener('change', e => {
                    selectedServer = e.target.value;
                    doSearch();
                });
                doSearch();
            });

            async function loadUserCount() {
                const res = await fetch('/api/user_count');
                const data = await res.json();
                document.getElementById('userCount').textContent = data.count.toLocaleString();
            }

            function onSearchInput() {
                if (searchTimeout) clearTimeout(searchTimeout);
                searchTimeout = setTimeout(doSearch, 250);
            }

            async function doSearch(offset = 0) {
                const q = document.getElementById('searchBox').value.trim();
                if (!q && !selectedServer) {
                    document.getElementById('results').innerHTML = "<span style='color:#888'>Search results will appear here.</span>";
                    document.getElementById('searchStats').style.display = 'none';
                    currentSearchResults = [];
                    return;
                }
                isLoading = true;
                document.getElementById('searchSpinner').classList.add('active');
                let url = `/api/search_user?limit=${currentLimit}&offset=${offset}`;
                if (q) url += `&q=${encodeURIComponent(q)}`;
                if (selectedServer) url += `&guild_id=${encodeURIComponent(selectedServer)}`;
                const res = await fetch(url);
                const data = await res.json();
                currentSearchResults = (offset === 0) ? data.results : currentSearchResults.concat(data.results);
                hasMoreResults = data.has_more;
                currentOffset = data.offset + data.limit;
                renderResults();
                renderStats(data.total, currentSearchResults.length, hasMoreResults);
                document.getElementById('searchSpinner').classList.remove('active');
                isLoading = false;
            }

            function renderResults() {
                const results = currentSearchResults;
                if (!results.length) {
                    document.getElementById('results').innerHTML = "<span style='color:#888'>No users found.</span>";
                    return;
                }
                let html = `<table><thead><tr><th>Name</th><th>Member ID</th><th>Account Created</th><th>Joined At</th><th>Server</th></tr></thead><tbody>`;
                results.forEach(row => {
                    html += `<tr class='user-row' style='cursor:pointer;' onclick='showUserHistory("${row.member_id}", "${row.name}")'>` +
                        `<td>${row.name}</td>` +
                        `<td>${row.member_id}</td>` +
                        `<td>${row.account_created}</td>` +
                        `<td>${row.joined_at}</td>` +
                        `<td>${row.guild_id}</td>` +
                        `</tr>`;
                });
                html += `</tbody></table>`;
                if (hasMoreResults) {
                    html += `<button class='load-more-btn' onclick='doSearch(${currentOffset})'>Load More</button>`;
                }
                document.getElementById('results').innerHTML = html;
            }

            function renderStats(total, shown, hasMore) {
                const stats = document.getElementById('searchStats');
                stats.textContent = `Showing ${shown.toLocaleString()} of ${total.toLocaleString()} result(s)` + (hasMore ? ' (scroll for more)' : '');
                stats.style.display = '';
            }

            function downloadCSV() {
                if (!currentSearchResults.length) return;
                let csv = 'Name,Member ID,Account Created,Joined At,Server\n';
                currentSearchResults.forEach(row => {
                    csv += `"${row.name}","${row.member_id}","${row.account_created}","${row.joined_at}","${row.guild_id}"\n`;
                });
                const blob = new Blob([csv], {type: 'text/csv'});
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'user_search_results.csv';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
            }

            function downloadJSON() {
                if (!currentSearchResults.length) return;
                const blob = new Blob([JSON.stringify(currentSearchResults, null, 2)], {type: 'application/json'});
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'user_search_results.json';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
            }

            async function showUserHistory(member_id, name) {
                document.getElementById('userHistoryModal').style.display = 'flex';
                document.getElementById('userHistoryTitle').textContent = `History for ${name} (${member_id})`;
                document.getElementById('userHistoryContent').innerHTML = '<div class="loading">Loading...</div>';
                const res = await fetch(`/api/user_history?member_id=${encodeURIComponent(member_id)}`);
                const data = await res.json();
                if (!data.length) {
                    document.getElementById('userHistoryContent').innerHTML = '<span style="color:#888">No history found for this user.</span>';
                    return;
                }
                let html = `<table style='width:100%;'><thead><tr><th>Server</th><th>Name</th><th>Account Created</th><th>Joined At</th></tr></thead><tbody>`;
                data.forEach(row => {
                    html += `<tr>` +
                        `<td>${row.guild_id}</td>` +
                        `<td>${row.name}</td>` +
                        `<td>${row.account_created}</td>` +
                        `<td>${row.joined_at}</td>` +
                        `</tr>`;
                });
                html += `</tbody></table>`;
                document.getElementById('userHistoryContent').innerHTML = html;
            }

            function closeUserHistory() {
                document.getElementById('userHistoryModal').style.display = 'none';
            }
        </script>
        <style>
            /* Hide sidebar scrollbar but keep scrollability */
            #sidebar { scrollbar-width: none; }
            #sidebar::-webkit-scrollbar { display: none; }
        </style>
    </body>
    </html>
    """)

@app.route('/api/search_user')
def search_user():
    q = request.args.get('q', '').strip()
    guild_id = request.args.get('guild_id', '').strip()
    limit = int(request.args.get('limit', 250))  # Default 250 results per page
    offset = int(request.args.get('offset', 0))  # Default start from beginning
    
    db = get_db()
    
    # Build WHERE clause
    where = []
    params = []
    if q:
        where.append('name LIKE ?')
        params.append('%' + q + '%')
    if guild_id:
        where.append('guild_id = ?')
        params.append(guild_id)
    where_clause = ' AND '.join(where) if where else '1=1'
    
    # Get total count first
    count_result = db.execute(
        f"SELECT COUNT(*) as count FROM demographics WHERE {where_clause}",
        params
    ).fetchone()
    total_count = count_result['count']
    
    # Get paginated results
    rows = db.execute(
        f"SELECT member_id, name, account_created, joined_at, guild_id FROM demographics WHERE {where_clause} ORDER BY name LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()
    
    def format_timestamp(ts):
        if not ts:
            return ''
        if isinstance(ts, str) and 'T' in ts:
            ts = ts.split('.')[0].replace('T', ' ')
            if '+' in ts:
                ts = ts.split('+')[0]
            return ts
        return ts
    
    results = [{
        'member_id': row['member_id'],
        'name': row['name'],
        'account_created': format_timestamp(row['account_created']),
        'joined_at': format_timestamp(row['joined_at']),
        'guild_id': row['guild_id']
    } for row in rows]
    
    has_more = (offset + limit) < total_count
    
    return jsonify({
        'results': results,
        'total': total_count,
        'has_more': has_more,
        'offset': offset,
        'limit': limit
    })

@app.route('/api/search_user_all')
def search_user_all():
    q = request.args.get('q', '').strip()
    guild_id = request.args.get('guild_id', '').strip()
    db = get_db()
    where = []
    params = []
    if q:
        where.append('name LIKE ?')
        params.append('%' + q + '%')
    if guild_id:
        where.append('guild_id = ?')
        params.append(guild_id)
    where_clause = ' AND '.join(where) if where else '1=1'
    rows = db.execute(
        f"SELECT member_id, name, account_created, joined_at, guild_id FROM demographics WHERE {where_clause} ORDER BY name",
        params
    ).fetchall()
    def format_timestamp(ts):
        if not ts:
            return ''
        if isinstance(ts, str) and 'T' in ts:
            ts = ts.split('.')[0].replace('T', ' ')
            if '+' in ts:
                ts = ts.split('+')[0]
            return ts
        return ts
    results = [{
        'member_id': row['member_id'],
        'name': row['name'],
        'account_created': format_timestamp(row['account_created']),
        'joined_at': format_timestamp(row['joined_at']),
        'guild_id': row['guild_id']
    } for row in rows]
    return jsonify(results)

@app.route('/config')
def config_page():
    return render_template_string(r"""
    <html>
    <head>
        <title>Server Config Editor</title>
        <style>
            body { display: flex; margin: 0; font-family: 'Segoe UI', Arial, sans-serif; background: #181a1b; }
            #sidebar {
                width: 220px;
                background: #23272a;
                color: #e0e0e0;
                height: 100vh;
                position: fixed;
                left: 0; top: 0; bottom: 0;
                overflow-y: auto;
                overflow-x: hidden;
                transition: width 0.2s;
                z-index: 10;
                font-family: inherit;
                scrollbar-width: none;
            }
            #sidebar::-webkit-scrollbar { display: none; }
            #sidebar.collapsed {
                width: 40px;
                min-width: 40px;
            }
            #sidebar.collapsed .toggle-btn {
                text-align: center;
                font-size: 2em;
                padding: 8px 0;
            }
            #sidebar.collapsed #serverList, #sidebar.collapsed #databaseLink, #sidebar.collapsed #configLink, #sidebar.collapsed #analyticsConfigLink {
                display: none;
            }
            #sidebar .toggle-btn {
                background: none; border: none; color: #90caf9; font-size: 1.5em; width: 100%; text-align: left; padding: 8px;
                cursor: pointer;
                outline: none;
            }
            #serverList { list-style: none; padding: 0; margin: 0; }
            #serverList li {
                padding: 12px 16px;
                cursor: pointer;
                border-bottom: 1px solid #333;
                transition: background 0.2s, color 0.2s;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                font-family: inherit;
            }
            #serverList li.active, #serverList li:hover { background: #181a1b; color: #90caf9; }
            #main-content {
                margin-left: 220px;
                padding: 24px;
                width: 100%;
                transition: margin-left 0.2s;
            }
            #sidebar.collapsed + #main-content { margin-left: 40px; }
            .config-table {
                width: 100%;
                border-collapse: collapse;
                background: #23272a;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 8px #000a;
            }
            .config-table th {
                background: #181a1b;
                color: #90caf9;
                padding: 12px;
                text-align: left;
                font-weight: bold;
            }
            .config-table td {
                padding: 12px;
                border-top: 1px solid #333;
            }
            .config-table tr:nth-child(even) {
                background: #23272a;
            }
            .config-table tr:nth-child(odd) {
                background: #202225;
            }
            .toggle-switch {
                position: relative;
                display: inline-block;
                width: 50px;
                height: 24px;
            }
            .toggle-switch input {
                opacity: 0;
                width: 0;
                height: 0;
            }
            .slider {
                position: absolute;
                cursor: pointer;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background-color: #666;
                transition: .4s;
                border-radius: 24px;
            }
            .slider:before {
                position: absolute;
                content: "";
                height: 18px;
                width: 18px;
                left: 3px;
                bottom: 3px;
                background-color: white;
                transition: .4s;
                border-radius: 50%;
            }
            input:checked + .slider {
                background-color: #90caf9;
            }
            input:checked + .slider:before {
                transform: translateX(26px);
            }
            .number-input {
                background: #181a1b;
                color: #e0e0e0;
                border: 1px solid #333;
                padding: 4px 8px;
                border-radius: 4px;
                width: 80px;
            }
            .notification {
                position: fixed;
                top: 20px;
                right: 20px;
                background: #4caf50;
                color: white;
                padding: 12px 20px;
                border-radius: 4px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.3);
                z-index: 1000;
                opacity: 0;
                transform: translateX(100%);
                transition: all 0.3s ease;
            }
            .notification.show {
                opacity: 1;
                transform: translateX(0);
            }
            .notification.error {
                background: #f44336;
            }
            .status-indicator {
                display: inline-block;
                width: 8px;
                height: 8px;
                border-radius: 50%;
                margin-right: 8px;
            }
            .status-active {
                background: #4caf50;
            }
            .status-inactive {
                background: #f44336;
            }
            .snapshot-btn {
                background: #90caf9;
                color: #181a1b;
                border: none;
                padding: 6px 12px;
                border-radius: 4px;
                cursor: pointer;
                font-weight: bold;
                font-size: 0.9em;
            }
            .snapshot-btn:hover {
                background: #64b5f6;
            }
            .snapshot-btn:disabled {
                background: #666;
                cursor: not-allowed;
            }
            .snapshot-btn.loading {
                background: #666;
                cursor: not-allowed;
            }
        </style>
    </head>
    <body>
        <div id="sidebar">
            <div style="display:flex;flex-direction:column;align-items:center;gap:4px;padding:16px 0 8px 0;">
                <a href="/" id="homeIcon" aria-label="Home" style="display:flex;justify-content:center;align-items:center;font-size:2em;color:#90caf9;text-decoration:none;margin:0 0 8px 0;">
                    <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#90caf9" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9.5L12 4l9 5.5V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V9.5z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
                </a>
                <button class="toggle-btn" onclick="toggleSidebar()" style="font-size:2em;padding:0;background:none;border:none;color:#90caf9;display:flex;justify-content:center;align-items:center;"></button>
            </div>
            <a href="/database" id="databaseLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:12px 16px 8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Database Search</a>
            <a href="/config" id="configLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Server Config</a>
            <ul id="serverList" style="padding-left:0;"></ul>
            <div style="margin-top: auto; padding-top: 20px; border-top: 1px solid #333;">
                <a href="/analytics-config" id="analyticsConfigLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Analytics Config</a>
            </div>
        </div>
        <div id="main-content">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:18px;">
                <h1 style="color:#90caf9;margin-top:0;">Server Configuration Editor</h1>
                <div>
                    <button id="snapshotAllBtn" class="snapshot-btn" style="font-size:1.05em;padding:10px 22px;">Snapshot All</button>
                    <button id="fetchAllBtn" class="snapshot-btn" style="font-size:1.05em;padding:10px 22px;background:#4caf50;margin-left:10px;">Fetch All</button>
                </div>
            </div>
            <div id="configTableContainer">
                <table class="config-table">
                    <thead>
                        <tr>
                            <th>Server Name</th>
                            <th>Auto Snapshot</th>
                            <th>Interval (hours)</th>
                            <th>Retention (days)</th>
                            <th>First Snapshot</th>
                            <th>Last Snapshot</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="configTableBody">
                        <tr><td colspan="8" style="text-align:center;color:#888;">Loading configurations...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
        <div id="notification" class="notification"></div>
        <script>
            let configs = [];
            
            function showNotification(message, isError = false) {
                const notification = document.getElementById('notification');
                notification.textContent = message;
                notification.className = `notification ${isError ? 'error' : ''}`;
                notification.classList.add('show');
                
                setTimeout(() => {
                    notification.classList.remove('show');
                }, 3000);
            }
            
            function toggleSidebar() {
                document.getElementById('sidebar').classList.toggle('collapsed');
                document.getElementById('main-content').classList.toggle('collapsed');
            }
            
            async function loadServers() {
                const res = await fetch('/api/servers');
                const servers = await res.json();
                const list = document.getElementById('serverList');
                list.innerHTML = '';
                let lastSelected = localStorage.getItem('nighty_last_server');
                let found = false;
                servers.forEach((srv, idx) => {
                    const li = document.createElement('li');
                    li.innerHTML = `<span class='server-name'>${srv.name || srv.id}</span>`;
                    li.onclick = () => {
                        localStorage.setItem('nighty_last_server', srv.id);
                        window.location.href = '/dashboard?guild_id=' + encodeURIComponent(srv.id);
                    };
                    list.appendChild(li);
                    if ((lastSelected && srv.id === lastSelected) || (!lastSelected && idx === 0)) {
                        li.classList.add('active');
                        found = true;
                    }
                });
                if (!found && servers.length > 0) {
                    list.firstChild.classList.add('active');
                }
            }
            
            function formatTimestamp(ts) {
                if (!ts) return 'Never';
                // Convert to string first
                const tsStr = String(ts);
                if (tsStr.includes('T')) {
                    const formatted = tsStr.split('.')[0].replace('T', ' ');
                    if (formatted.includes('+')) {
                        return formatted.split('+')[0];
                    }
                    return formatted;
                }
                return tsStr;
            }
            
            function formatTimeSince(ts) {
                if (!ts) return 'Never';
                const tsStr = String(ts);
                const timestamp = new Date(tsStr);
                const now = new Date();
                const diffMs = now - timestamp;
                const diffMins = Math.floor(diffMs / (1000 * 60));
                const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
                const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
                
                if (diffMins < 1) return 'Just now';
                if (diffMins < 60) return `${diffMins}m ago`;
                if (diffHours < 24) return `${diffHours}h ago`;
                if (diffDays < 7) return `${diffDays}d ago`;
                
                // For longer periods, show the actual date
                return formatTimestamp(ts);
            }
            
            async function loadConfigs() {
                try {
                    const res = await fetch('/api/server_configs');
                    if (!res.ok) {
                        const errorData = await res.json();
                        throw new Error(errorData.error || `HTTP ${res.status}`);
                    }
                    configs = await res.json();
                    renderConfigTable();
                } catch (e) {
                    console.error('Error loading configs:', e);
                    document.getElementById('configTableBody').innerHTML = 
                        `<tr><td colspan="8" style="text-align:center;color:#f44336;">Error loading configurations: ${e.message}</td></tr>`;
                }
            }
            
            function renderConfigTable() {
                const tbody = document.getElementById('configTableBody');
                if (!configs.length) {
                    tbody.innerHTML = `
                        <tr>
                            <td colspan="8" style="text-align:center;color:#888;">
                                <div style="margin-bottom: 10px;">No server configurations found</div>
                                <div style="font-size: 0.9em; color: #666;">
                                    Requirements: Server must have a first snapshot date in the database.<br>
                                    <a href="/api/debug_configs" target="_blank" style="color: #90caf9;">Debug: Check database contents</a>
                                </div>
                            </td>
                        </tr>
                    `;
                    return;
                }
                tbody.innerHTML = configs.map(config => `
                    <tr data-guild-id="${config.guild_id}">
                        <td>${config.guild_name}</td>
                        <td>
                            <span class="status-indicator ${config.auto_snapshot ? 'status-active' : 'status-inactive'}"></span>
                            <label class="toggle-switch">
                                <input type="checkbox" ${config.auto_snapshot ? 'checked' : ''} 
                                    onchange="updateConfig('${config.guild_id}', 'auto_snapshot', this.checked)">
                                <span class="slider"></span>
                            </label>
                        </td>
                        <td>
                            <input type="number" class="number-input" value="${config.auto_snapshot_interval_hours || 20}" 
                                min="1" max="168" step="1" 
                                onchange="updateConfig('${config.guild_id}', 'auto_snapshot_interval_hours', this.value)">
                        </td>
                        <td>
                            <input type="number" class="number-input" value="${config.snapshot_retention_days || 90}" 
                                min="1" max="365" 
                                onchange="updateConfig('${config.guild_id}', 'snapshot_retention_days', this.value)">
                        </td>
                        <td>${formatTimestamp(config.first_snapshot_date)}</td>
                        <td>${formatTimeSince(config.last_auto_snapshot)}</td>
                        <td>
                            <button class="snapshot-btn" onclick="takeSnapshot('${config.guild_id}')">Take Snapshot</button>
                            <br><br>
                            <button class="snapshot-btn" onclick="fetchMembers('${config.guild_id}')" style="background: #4caf50;">Fetch Members</button>
                        </td>
                    </tr>
                `).join('');
            }
            
            async function updateConfig(guildId, field, value) {
                try {
                    const res = await fetch('/api/update_config', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({guild_id: guildId, field: field, value: value})
                    });
                    
                    if (!res.ok) {
                        throw new Error('Failed to update config');
                    }
                    
                    // Update local config
                    const config = configs.find(c => c.guild_id === guildId);
                    if (config) {
                        config[field] = value;
                    }
                    
                    // Update the status indicator immediately for auto_snapshot changes
                    if (field === 'auto_snapshot') {
                        const row = document.querySelector(`tr[data-guild-id="${guildId}"]`);
                        const statusIndicator = row.querySelector('.status-indicator');
                        if (value) {
                            statusIndicator.className = 'status-indicator status-active';
                        } else {
                            statusIndicator.className = 'status-indicator status-inactive';
                        }
                    }
                    
                    // Show specific notification based on field
                    let fieldName = field.replace('_', ' ');
                    if (field === 'auto_snapshot') {
                        fieldName = value ? 'Auto snapshot enabled' : 'Auto snapshot disabled';
                    } else if (field === 'auto_snapshot_interval_hours') {
                        fieldName = `Snapshot interval set to ${value} hours`;
                    } else if (field === 'snapshot_retention_days') {
                        fieldName = `Retention set to ${value} days`;
                    }
                    
                    showNotification(fieldName);
                    
                } catch (e) {
                    console.error('Error updating config:', e);
                    showNotification('Failed to update configuration', true);
                }
            }
            
            async function takeSnapshot(guildId) {
                const button = document.querySelector(`tr[data-guild-id="${guildId}"] .snapshot-btn`);
                const originalText = button.textContent;
                
                // Show loading state
                button.textContent = 'Taking Snapshot...';
                button.disabled = true;
                button.classList.add('loading');
                
                try {
                    const res = await fetch('/api/take_snapshot/' + guildId, {
                        method: 'POST'
                    });
                    
                    if (!res.ok) {
                        const errorData = await res.json();
                        throw new Error(errorData.error || 'Failed to take snapshot');
                    }
                    
                    const data = await res.json();
                    
                    if (data.success) {
                        const memberCount = data.member_count || 'unknown';
                        showNotification(`Snapshot taken successfully! Member count: ${memberCount}`, false);
                        
                        // Reload configs to update last_auto_snapshot
                        await loadConfigs();
                    } else {
                        throw new Error(data.error || 'Unknown error occurred');
                    }
                    
                } catch (e) {
                    console.error('Snapshot error:', e);
                    showNotification(`Failed to take snapshot: ${e.message}`, true);
                } finally {
                    // Restore button state
                    button.textContent = originalText;
                    button.disabled = false;
                    button.classList.remove('loading');
                }
            }
            
            async function fetchMembers(guildId) {
                const button = document.querySelector(`tr[data-guild-id="${guildId}"] .snapshot-btn[onclick*="fetchMembers"]`);
                const originalText = button.textContent;
                
                // Show loading state
                button.textContent = 'Fetching Members...';
                button.disabled = true;
                button.classList.add('loading');
                
                try {
                    const res = await fetch('/api/fetch_members/' + guildId, {
                        method: 'POST'
                    });
                    
                    if (!res.ok) {
                        const errorData = await res.json();
                        throw new Error(errorData.error || 'Failed to fetch members');
                    }
                    
                    const data = await res.json();
                    
                    if (data.success) {
                        showNotification(`Member fetch triggered! The NightyScript will process this request.`, false);
                    } else {
                        throw new Error(data.error || 'Unknown error occurred');
                    }
                    
                } catch (e) {
                    console.error('Fetch members error:', e);
                    showNotification(`Failed to fetch members: ${e.message}`, true);
                } finally {
                    // Restore button state
                    button.textContent = originalText;
                    button.disabled = false;
                    button.classList.remove('loading');
                }
            }
            
            async function testAutoSnapshot(guildId) {
                const button = document.querySelector(`tr[data-guild-id="${guildId}"] .snapshot-btn[onclick*="testAutoSnapshot"]`);
                const originalText = button.textContent;
                
                // Show loading state
                button.textContent = 'Testing...';
                button.disabled = true;
                button.classList.add('loading');
                
                try {
                    const res = await fetch('/api/test_auto_snapshot/' + guildId, {
                        method: 'POST'
                    });
                    
                    if (!res.ok) {
                        const errorData = await res.json();
                        throw new Error(errorData.error || 'Failed to test auto snapshot');
                    }
                    
                    const data = await res.json();
                    
                    if (data.success) {
                        showNotification(`Test auto snapshot notification sent successfully! Check your webhook.`, false);
                    } else {
                        throw new Error(data.error || 'Unknown error occurred');
                    }
                    
                } catch (e) {
                    console.error('Test auto snapshot error:', e);
                    showNotification(`Failed to test auto snapshot: ${e.message}`, true);
                } finally {
                    // Restore button state
                    button.textContent = originalText;
                    button.disabled = false;
                    button.classList.remove('loading');
                }
            }
            
            async function snapshotAllServers() {
                const btn = document.getElementById('snapshotAllBtn');
                btn.disabled = true;
                btn.textContent = 'Snapshotting...';
                try {
                    const res = await fetch('/api/snapshot_all', { method: 'POST' });
                    const data = await res.json();
                    if (data.success) {
                        showNotification(`Snapshot taken for all servers! (${data.count} servers)`);
                        await loadConfigs();
                    } else {
                        showNotification(data.error || 'Snapshot all failed', true);
                    }
                } catch (e) {
                    showNotification('Snapshot all failed', true);
                } finally {
                    btn.disabled = false;
                    btn.textContent = 'Snapshot All';
                }
            }
            document.getElementById('snapshotAllBtn').addEventListener('click', snapshotAllServers);
            
            async function fetchAllServers() {
                const btn = document.getElementById('fetchAllBtn');
                btn.disabled = true;
                btn.textContent = 'Fetching...';
                try {
                    const res = await fetch('/api/fetch_all', { method: 'POST' });
                    const data = await res.json();
                    if (data.success) {
                        showNotification(`Fetched members for all servers! (${data.count} servers)`);
                        await loadConfigs();
                    } else {
                        showNotification(data.error || 'Fetch all failed', true);
                    }
                } catch (e) {
                    showNotification('Fetch all failed', true);
                } finally {
                    btn.disabled = false;
                    btn.textContent = 'Fetch All';
                }
            }
            document.getElementById('fetchAllBtn').addEventListener('click', fetchAllServers);
            
            loadServers();
            loadConfigs();
        </script>
    </body>
    </html>
    """)

@app.route('/api/test_db')
def test_db():
    try:
        db = get_db()
        # Check if server_config table exists
        tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='server_config'").fetchall()
        if not tables:
            return jsonify({'error': 'server_config table does not exist'}), 404
        
        # Check if there are any rows
        count = db.execute("SELECT COUNT(*) as count FROM server_config").fetchone()
        return jsonify({
            'success': True,
            'table_exists': True,
            'row_count': count['count'],
            'db_path': DB_PATH
        })
    except Exception as e:
        return jsonify({'error': str(e), 'db_path': DB_PATH}), 500

@app.route('/api/debug_configs')
def debug_configs():
    try:
        db = get_db()
        
        # Check all server_config entries
        all_configs = db.execute('SELECT * FROM server_config').fetchall()
        
        # Check snapshots table for guild names
        snapshots = db.execute('SELECT guild_id, guild_name, timestamp FROM snapshots ORDER BY guild_id, timestamp DESC').fetchall()
        
        # Check which servers have configs but no first_snapshot_date
        configs_without_date = db.execute('SELECT guild_id FROM server_config WHERE first_snapshot_date IS NULL').fetchall()
        
        return jsonify({
            'all_configs': [dict(row) for row in all_configs],
            'snapshots': [dict(row) for row in snapshots],
            'configs_without_date': [row['guild_id'] for row in configs_without_date],
            'total_configs': len(all_configs),
            'total_snapshots': len(snapshots)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/analytics-config')
def analytics_config_page():
    return render_template_string(r"""
    <html>
    <head>
        <title>Analytics Configuration</title>
        <style>
            body { display: flex; margin: 0; font-family: 'Segoe UI', Arial, sans-serif; background: #181a1b; }
            #sidebar {
                width: 220px;
                background: #23272a;
                color: #e0e0e0;
                height: 100vh;
                position: fixed;
                left: 0; top: 0; bottom: 0;
                overflow-y: auto;
                overflow-x: hidden;
                transition: width 0.2s;
                z-index: 10;
                font-family: inherit;
                scrollbar-width: none;
            }
            #sidebar::-webkit-scrollbar { display: none; }
            #sidebar.collapsed {
                width: 40px;
                min-width: 40px;
            }
            #sidebar.collapsed .toggle-btn {
                text-align: center;
                font-size: 2em;
                padding: 8px 0;
            }
            #sidebar.collapsed #serverList, #sidebar.collapsed #databaseLink, #sidebar.collapsed #configLink, #sidebar.collapsed #analyticsConfigLink {
                display: none;
            }
            #sidebar .toggle-btn {
                background: none; border: none; color: #90caf9; font-size: 1.5em; width: 100%; text-align: left; padding: 8px;
                cursor: pointer;
                outline: none;
            }
            #serverList { list-style: none; padding: 0; margin: 0; }
            #serverList li {
                padding: 12px 16px;
                cursor: pointer;
                border-bottom: 1px solid #333;
                transition: background 0.2s, color 0.2s;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                font-family: inherit;
            }
            #serverList li.active, #serverList li:hover { background: #181a1b; color: #90caf9; }
            #main-content {
                margin-left: 220px;
                padding: 24px;
                width: 100%;
                transition: margin-left 0.2s;
            }
            #sidebar.collapsed + #main-content { margin-left: 40px; }
            .config-box {
                background: #23272a;
                color: #e0e0e0;
                border-radius: 8px;
                padding: 24px;
                margin-bottom: 20px;
                box-shadow: 0 2px 8px #000a;
            }
            .config-title {
                color: #90caf9;
                font-weight: bold;
                margin-bottom: 16px;
                font-size: 1.2em;
            }
        </style>
    </head>
    <body>
        <div id="sidebar">
            <div style="display:flex;flex-direction:column;align-items:center;gap:4px;padding:16px 0 8px 0;">
                <a href="/" id="homeIcon" aria-label="Home" style="display:flex;justify-content:center;align-items:center;font-size:2em;color:#90caf9;text-decoration:none;margin:0 0 8px 0;">
                    <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#90caf9" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9.5L12 4l9 5.5V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V9.5z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
                </a>
                <button class="toggle-btn" onclick="toggleSidebar()" style="font-size:2em;padding:0;background:none;border:none;color:#90caf9;display:flex;justify-content:center;align-items:center;"></button>
            </div>
            <a href="/database" id="databaseLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:12px 16px 8px 16px;cursor:pointer;text-align:left;font-family:inherit;"> Database Search</a>
            <a href="/config" id="configLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Server Config</a>
            <ul id="serverList" style="padding-left:0;"></ul>
            <div style="margin-top: auto; padding-top: 20px; border-top: 1px solid #333;">
                <a href="/analytics-config" id="analyticsConfigLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Analytics Config</a>
            </div>
        </div>
        <div id="main-content">
            <h1 style="color:#90caf9;margin-top:0;">Analytics Configuration</h1>
            <div class="config-box">
                <div class="config-title">Global Analytics Settings</div>
                <form id="webhookForm">
                    <label for="webhook_url">Webhook URL for Logging:</label><br>
                    <input type="text" id="webhook_url" name="webhook_url" style="width:100%;padding:8px;margin:8px 0 16px 0;border-radius:4px;border:1px solid #333;background:#181a1b;color:#e0e0e0;" placeholder="https://discord.com/api/webhooks/..." />
                    <button type="submit" style="background:#90caf9;color:#181a1b;padding:8px 18px;border:none;border-radius:4px;cursor:pointer;font-weight:bold;">Save Webhook</button>
                    <button type="button" id="testWebhookBtn" style="background:#23272a;color:#90caf9;padding:8px 18px;border:1px solid #90caf9;border-radius:4px;cursor:pointer;font-weight:bold;margin-left:10px;">Test Webhook</button>
                    <span id="webhookStatus" style="margin-left:16px;"></span>
                </form>
                <p>Analytics configuration page coming soon...</p>
                <p>This page will allow you to configure:</p>
                <ul>
                    <li>Global timezone settings</li>
                    <li>Default chart styles</li>
                    <li>Data retention policies</li>
                    <li>Export settings</li>
                    <li>Notification preferences</li>
                </ul>
            </div>
        </div>
        <script>
            function toggleSidebar() {
                document.getElementById('sidebar').classList.toggle('collapsed');
                document.getElementById('main-content').classList.toggle('collapsed');
            }
            async function loadServers() {
                const res = await fetch('/api/servers');
                const servers = await res.json();
                const list = document.getElementById('serverList');
                list.innerHTML = '';
                let lastSelected = localStorage.getItem('nighty_last_server');
                let found = false;
                servers.forEach((srv, idx) => {
                    const li = document.createElement('li');
                    li.innerHTML = `<span class='server-name'>${srv.name || srv.id}</span>`;
                    li.onclick = () => {
                        localStorage.setItem('nighty_last_server', srv.id);
                        window.location.href = '/dashboard?guild_id=' + encodeURIComponent(srv.id);
                    };
                    list.appendChild(li);
                    if ((lastSelected && srv.id === lastSelected) || (!lastSelected && idx === 0)) {
                        li.classList.add('active');
                        found = true;
                    }
                });
                if (!found && servers.length > 0) {
                    list.firstChild.classList.add('active');
                }
            }
            loadServers();
            // Webhook form logic
            document.getElementById('webhookForm').addEventListener('submit', async function(e) {
                e.preventDefault();
                const url = document.getElementById('webhook_url').value.trim();
                const status = document.getElementById('webhookStatus');
                status.textContent = 'Saving...';
                status.style.color = '#90caf9';
                try {
                    const res = await fetch('/api/analytics_webhook', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ webhook_url: url })
                    });
                    const data = await res.json();
                    if (data.success) {
                        status.textContent = 'Saved!';
                        status.style.color = '#90caf9';
                    } else {
                        status.textContent = data.error || 'Error saving webhook';
                        status.style.color = '#f44336';
                    }
                } catch (err) {
                    status.textContent = 'Network error';
                    status.style.color = '#f44336';
                }
            });
            document.getElementById('testWebhookBtn').addEventListener('click', async function() {
                const status = document.getElementById('webhookStatus');
                status.textContent = 'Testing...';
                status.style.color = '#90caf9';
                try {
                    const res = await fetch('/api/analytics_webhook/test', { method: 'POST' });
                    const data = await res.json();
                    if (data.success) {
                        status.textContent = 'Test message sent!';
                        status.style.color = '#4caf50';
                    } else {
                        status.textContent = data.error || 'Test failed';
                        status.style.color = '#f44336';
                    }
                } catch (err) {
                    status.textContent = 'Network error';
                    status.style.color = '#f44336';
                }
            });
            // Load current webhook value
            fetch('/api/analytics_webhook').then(res => res.json()).then(data => {
                if (data.webhook_url) {
                    document.getElementById('webhook_url').value = data.webhook_url;
                }
            });
        </script>
    </body>
    </html>
    """)

@app.route('/api/fetch_members/<guild_id>', methods=['POST'])
def trigger_fetch_members(guild_id):
    """Trigger member fetching for a specific server via the NightyScript micro-API"""
    try:
        import requests
        import os
        from datetime import datetime, timezone
        # Get the API token from environment variable
        api_token = os.environ.get('NIGHTY_API_TOKEN', 'default_token_change_me')
        # Prepare the request to the NightyScript micro-API
        api_url = f'{NIGHTY_API_BASE_URL}/fetch_members'
        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }
        data = {
            'guild_id': str(guild_id),
            'token': api_token
        }
        # Accept channel_id from POST body if provided
        if request.is_json:
            body = request.get_json(silent=True) or {}
            channel_id = body.get('channel_id')
            if channel_id:
                data['channel_id'] = channel_id
        # Make the request to the NightyScript micro-API
        response = requests.post(api_url, headers=headers, json=data, timeout=30)
        if response.status_code == 200:
            result = response.json()
            # Webhook log on success
            guild_name = result.get('guild_name', f'Server {guild_id}')
            member_count = result.get('members_fetched', 0)
            now = datetime.now(timezone.utc)
            timestamp = now.strftime('%Y-%m-%d %H:%M:%S UTC')
            embed = {
                "title": "Fetched Members",
                "description": f"Fetched members for {guild_name} at {timestamp}",
                "fields": [
                    {"name": "Member Count", "value": str(member_count), "inline": True},
                    {"name": "Server ID", "value": str(guild_id), "inline": True}
                ],
                "color": 0x90caf9
            }
            send_webhook_log("", embed=embed)
            return jsonify({
                'success': True,
                'message': result.get('message', f'Member fetch completed for server {guild_id}'),
                'members_fetched': member_count,
                'guild_name': guild_name
            })
        else:
            error_data = response.json() if response.content else {'error': 'Unknown error'}
            return jsonify({
                'success': False,
                'error': error_data.get('error', f'HTTP {response.status_code}')
            }), response.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({
            'success': False,
            'error': 'Cannot connect to NightyScript micro-API. Make sure the NightyScript is running.'
        }), 503
    except requests.exceptions.Timeout:
        return jsonify({
            'success': False,
            'error': 'Request to NightyScript micro-API timed out.'
        }), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/server/<guild_id>/stats')
def server_stats(guild_id):
    db = get_db()
    import datetime
    # Get all snapshots for this server
    rows = db.execute('SELECT timestamp, member_count, boosters FROM snapshots WHERE guild_id=? ORDER BY timestamp', (guild_id,)).fetchall()
    if not rows:
        return jsonify({})
    member_counts = [row['member_count'] for row in rows]
    booster_counts = [row['boosters'] if 'boosters' in row.keys() else None for row in rows]
    timestamps = [row['timestamp'] for row in rows]
    peak = max(member_counts)
    peak_idx = member_counts.index(peak)
    peak_date = timestamps[peak_idx][:16].replace('T', ' ')
    current = member_counts[-1]
    current_boosters = booster_counts[-1] if booster_counts[-1] is not None else 0
    first = member_counts[0]
    last_snapshot = timestamps[-1][:16].replace('T', ' ')
    # Time since last snapshot
    last_dt = datetime.datetime.fromisoformat(timestamps[-1])
    now = datetime.datetime.now(datetime.timezone.utc)
    delta = now - last_dt.replace(tzinfo=datetime.timezone.utc) if last_dt.tzinfo is None else now - last_dt
    mins = int(delta.total_seconds() // 60)
    if mins < 60:
        time_since = f"{mins} minute{'s' if mins != 1 else ''} ago"
    else:
        hours = mins // 60
        time_since = f"{hours} hour{'s' if hours != 1 else ''} ago"
    return jsonify({
        'peak_member_count': peak,
        'peak_member_date': peak_date,
        'current_member_count': current,
        'current_boosters': current_boosters,
        'change_since_first': f"{current - first:+}",
        'last_snapshot': last_snapshot,
        'time_since_last': time_since,
        'total_snapshots': len(rows)
    })

@app.route('/api/analytics_webhook', methods=['GET', 'POST'])
def analytics_webhook_api():
    if request.method == 'POST':
        data = request.json
        url = data.get('webhook_url', '').strip()
        if not url or not url.startswith('http'):
            return jsonify({'success': False, 'error': 'Invalid URL'}), 400
        set_global_webhook_url(url)
        return jsonify({'success': True})
    else:
        url = get_global_webhook_url()
        return jsonify({'webhook_url': url})

@app.route('/api/analytics_webhook/test', methods=['POST'])
def analytics_webhook_test():
    url = get_global_webhook_url()
    if not url:
        return jsonify({'success': False, 'error': 'No webhook URL set'}), 400
    try:
        resp = requests.post(url, json={"content": "Test message from Nighty Analytics Dashboard."}, timeout=5)
        if resp.status_code in (200, 204):
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': f'Webhook returned status {resp.status_code}'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

def send_webhook_log(message, embed=None):
    url = get_global_webhook_url()
    if not url:
        return False
    try:
        payload = {"content": message}
        if embed:
            payload["embeds"] = [embed]
        requests.post(url, json=payload, timeout=5)
        return True
    except Exception:
        return False

@app.route('/api/server/<guild_id>/channels')
def server_channels(guild_id):
    db = get_db()
    # Try to get the latest snapshot for this server
    row = db.execute(
        'SELECT timestamp FROM snapshots WHERE guild_id = ? ORDER BY timestamp DESC LIMIT 1',
        (guild_id,)
    ).fetchone()
    if not row:
        return jsonify([])
    # Try to load channel info from JSON if it exists
    import os, json
    server_dir = os.path.join(os.path.dirname(__file__), 'server_member_tracking', str(guild_id))
    channels_path = os.path.join(server_dir, 'channels.json')
    channels = []
    if os.path.exists(channels_path):
        try:
            with open(channels_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Expecting a list of {id, name, type}
                channels = [ch for ch in data if ch.get('type') == 'text']
        except Exception:
            pass
    # If no channels.json, fallback to empty list
    return jsonify([{'id': ch['id'], 'name': ch['name']} for ch in channels])

@app.route('/api/24hr_stats')
def stats_24hr():
    db = get_db()
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc)
    day_ago = now - datetime.timedelta(hours=24)
    # Snapshots in last 24h
    snap_24h = db.execute("SELECT COUNT(*) as count FROM snapshots WHERE timestamp >= ?", (day_ago.isoformat(),)).fetchone()[0]
    snap_total = db.execute("SELECT COUNT(*) as count FROM snapshots").fetchone()[0]
    snap_24h_ago = db.execute("SELECT COUNT(*) as count FROM snapshots WHERE timestamp < ?", (day_ago.isoformat(),)).fetchone()[0]
    # Tracked servers (with at least one snapshot)
    servers_total = db.execute("SELECT COUNT(DISTINCT guild_id) as count FROM snapshots").fetchone()[0]
    servers_24h = db.execute("SELECT COUNT(DISTINCT guild_id) as count FROM snapshots WHERE timestamp >= ?", (day_ago.isoformat(),)).fetchone()[0]
    servers_24h_ago = db.execute("SELECT COUNT(DISTINCT guild_id) as count FROM snapshots WHERE timestamp < ?", (day_ago.isoformat(),)).fetchone()[0]
    # Total memberships (all entries in demographics)
    memberships_total = db.execute("SELECT COUNT(*) as count FROM demographics").fetchone()[0]
    memberships_24h = db.execute("SELECT COUNT(*) as count FROM demographics WHERE joined_at >= ? OR account_created >= ?", (day_ago.isoformat(), day_ago.isoformat())).fetchone()[0]
    memberships_24h_ago = db.execute("SELECT COUNT(*) as count FROM demographics WHERE (joined_at < ? OR (account_created < ? AND (joined_at IS NULL OR joined_at = '')))", (day_ago.isoformat(), day_ago.isoformat())).fetchone()[0]
    return jsonify({
        'snapshots': {
            'total': snap_total,
            'delta': snap_total - snap_24h_ago,
            'last_24h': snap_24h
        },
        'servers': {
            'total': servers_total,
            'delta': servers_total - servers_24h_ago,
            'last_24h': servers_24h
        },
        'members': {
            'total': memberships_total,
            'delta': memberships_total - memberships_24h_ago,
            'last_24h': memberships_24h
        }
    })

@app.route('/dashboard')
def dashboard():
    guild_id = request.args.get('guild_id')
    # Render the per-server dashboard, using the same HTML/JS as the old root dashboard
    return render_template_string(r'''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Server Analytics Dashboard</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body { display: flex; margin: 0; font-family: 'Segoe UI', Arial, sans-serif; background: #181a1b; font-size: 14px; }
            /* Hide scrollbars but keep scrollability */
            body::-webkit-scrollbar, #sidebar::-webkit-scrollbar, #main-content::-webkit-scrollbar {
                display: none;
            }
            body, #sidebar, #main-content {
                -ms-overflow-style: none;
                scrollbar-width: none;
            }
            /* Prevent horizontal scroll on main content */
            #main-content, html, body {
                overflow-x: hidden;
            }
            #sidebar {
                width: 220px;
                background: #23272a;
                color: #e0e0e0;
                height: 100vh;
                position: fixed;
                left: 0; top: 0; bottom: 0;
                overflow-y: auto;
                overflow-x: hidden;
                transition: width 0.2s;
                z-index: 10;
                font-family: inherit;
                scrollbar-width: none;
            }
            #sidebar::-webkit-scrollbar { display: none; }
            #sidebar.collapsed {
                width: 40px;
                min-width: 40px;
            }
            #sidebar .toggle-btn {
                background: none; border: none; color: #90caf9; font-size: 1.5em; width: 100%; text-align: left; padding: 8px;
                cursor: pointer;
                outline: none;
            }
            #sidebar .toggle-btn {
                text-align: center;
                font-size: 2em;
                padding: 8px 0;
            }
            #sidebar #homeIcon {
                display: block;
                text-align: center;
                font-size: 2em;
                padding: 10px 0 6px 0;
                color: #90caf9;
                text-decoration: none;
                margin-bottom: 2px;
            }
            #sidebar.collapsed #homeIcon svg {
                margin: 0 auto;
            }
            #sidebar.collapsed #serverList,
            #sidebar.collapsed #databaseLink,
            #sidebar.collapsed #configLink,
            #sidebar.collapsed #analyticsConfigLink {
                display: none !important;
            }
            #sidebar.collapsed #serverList li,
            #sidebar.collapsed #databaseLink,
            #sidebar.collapsed #configLink,
            #sidebar.collapsed #analyticsConfigLink {
                pointer-events: none;
            }
            #serverList { list-style: none; padding: 0; margin: 0; }
            #serverList li {
                padding: 12px 16px;
                cursor: pointer;
                border-bottom: 1px solid #333;
                transition: background 0.2s, color 0.2s;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                font-family: inherit;
            }
            #serverList li.active, #serverList li:hover { background: #181a1b; color: #90caf9; }
            #main-content {
                margin-left: 220px;
                padding: 40px 32px;
                width: 100%;
                min-height: 100vh;
                background: #181a1b;
            }
            #sidebar.collapsed + #main-content { margin-left: 40px; }
            h1, label {
                color: #90caf9;
                font-size: 1.15em;
                font-weight: 500;
            }
            .dashboard-row {
                display: flex;
                flex-wrap: wrap;
                gap: 32px;
                align-items: flex-start;
                margin-bottom: 32px;
            }
            .stat-list {
                background: #23272a;
                color: #e0e0e0;
                border-radius: 8px;
                padding: 18px 20px 18px 20px;
                min-width: 180px;
                max-width: 260px;
                box-shadow: 0 2px 8px #000a;
                font-size: 0.97em;
                flex: 0 0 210px;
            }
            .stat-list ul {
                list-style: none;
                padding: 0;
                margin: 0;
            }
            .stat-list li {
                margin-bottom: 10px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-size: 0.97em;
            }
            .stat-label {
                color: #90caf9;
                font-size: 0.97em;
                font-weight: 400;
            }
            .stat-value {
                font-size: 1.05em;
                font-weight: 500;
                color: #e0e0e0;
                margin-left: 8px;
            }
            .chart-box {
                background: #23272a;
                border-radius: 8px;
                padding: 24px;
                box-shadow: 0 2px 8px #000a;
                flex: 1 1 400px;
                min-width: 320px;
                max-width: 100%;
                margin-bottom: 0;
            }
            .chart-title {
                color: #90caf9;
                font-size: 0.98em;
                margin-bottom: 6px;
                font-weight: 500;
            }
            .chart-box canvas {
                width: 100% !important;
                height: 260px !important;
                max-width: 100%;
            }
            .full-width-chart {
                background: #23272a;
                border-radius: 8px;
                padding: 24px;
                margin-top: 0;
                box-shadow: 0 2px 8px #000a;
                flex: 1 1 400px;
                min-width: 320px;
                max-width: 100%;
                margin-left: 0;
                margin-right: 0;
                display: flex;
                flex-direction: column;
                align-items: stretch;
            }
            .full-width-chart canvas {
                width: 100% !important;
                height: 260px !important;
                max-width: 100%;
                display: block;
            }
        </style>
    </head>
    <body>
        <div id="sidebar">
            <div style="display:flex;flex-direction:column;align-items:center;gap:4px;padding:16px 0 8px 0;">
                <a href="/" id="homeIcon" aria-label="Home" style="display:flex;justify-content:center;align-items:center;font-size:2em;color:#90caf9;text-decoration:none;margin:0 0 8px 0;">
                    <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#90caf9" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9.5L12 4l9 5.5V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V9.5z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
                </a>
                <button class="toggle-btn" onclick="toggleSidebar()" style="font-size:2em;padding:0;background:none;border:none;color:#90caf9;display:flex;justify-content:center;align-items:center;"></button>
            </div>
            <a href="/database" id="databaseLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:12px 16px 8px 16px;cursor:pointer;text-align:left;font-family:inherit;"> Database Search</a>
            <a href="/config" id="configLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Server Config</a>
            <ul id="serverList"></ul>
            <div style="margin-top: auto; padding-top: 20px; border-top: 1px solid #333;">
                <a href="/analytics-config" id="analyticsConfigLink" style="display:block;text-decoration:none;color:#90caf9;background:none;border:none;font-size:1.1em;padding:8px 16px;cursor:pointer;text-align:left;font-family:inherit;">Analytics Config</a>
            </div>
        </div>
        <div id="main-content">
            <h1>Analytics Overview</h1>
            <div class="dashboard-row">
                <div id="serverStatsCard" class="stat-list" style="display:none;min-width:320px;max-width:400px;">
                    <div style="font-weight:bold;font-size:1.2em;color:#90caf9;margin-bottom:10px;display:flex;align-items:center;justify-content:space-between;">
                        <span>Server Stats</span>
                        <span id="peakMemberDate" style="color:#aaa;font-size:0.95em;margin-left:16px;"></span>
                    </div>
                    <ul style="font-size:1em;">
                        <li><span class="stat-label">Peak Member Count:</span> <span class="stat-value" id="peakMemberCount">...</span></li>
                        <li><span class="stat-label">Current Member Count:</span> <span class="stat-value" id="currentMemberCount">...</span></li>
                        <li><span class="stat-label">Booster Count:</span> <span class="stat-value" id="boosterCount">...</span></li>
                        <li><span class="stat-label">Change since First Snapshot:</span> <span class="stat-value" id="changeSinceFirst">...</span></li>
                        <li><span class="stat-label">Last Snapshot:</span> <span class="stat-value" id="lastSnapshot">...</span></li>
                        <li><span class="stat-label">Time Since Last Snapshot:</span> <span class="stat-value" id="timeSinceLast">...</span></li>
                        <li><span class="stat-label">Total Snapshots:</span> <span class="stat-value" id="totalSnapshots">...</span></li>
                    </ul>
                </div>
                <div class="chart-box">
                    <div class="chart-title">Snapshots Taken (Last 24 Hours)</div>
                    <canvas id="snapshots24hChart"></canvas>
                </div>
            </div>
            <!-- New flex row for both charts -->
            <div class="dashboard-row" style="margin-top:0;">
                <div class="full-width-chart">
                    <div style="margin-bottom: 10px;">
                        <label for="membersDaysSelect" style="color:#90caf9;">Show last</label>
                        <select id="membersDaysSelect">
                            <option value="7" selected>7 days</option>
                            <option value="14">14 days</option>
                            <option value="30">30 days</option>
                            <option value="90">90 days</option>
                            <option value="all">All</option>
                        </select>
                    </div>
                    <div class="chart-title">Unique Members Tracked Over Time</div>
                    <canvas id="membersOverTimeChart"></canvas>
                </div>
            </div>
            <div id="demographicsSection" style="display:none; margin-top:32px;">
                <div class="demographics-collapsible" style="background:#23272a;color:#e0e0e0;border-radius:8px;padding:20px 28px;margin-bottom:18px;box-shadow:0 2px 8px #000a;">
                    <div style="cursor:pointer;font-weight:bold;font-size:1.1em;color:#90caf9;" onclick="toggleDemographics()">
                        Demographics <span id="demoCollapseIcon">+</span>
                    </div>
                    <div id="demographicsContent" style="display:none;margin-top:16px;">
                        <div id="demographicsLists">Loading...</div>
                    </div>
                </div>
            </div>
            <div id="snapshotLogSection" style="display:none;">
                <div class="snapshot-log-box" style="background:#23272a;color:#e0e0e0;border-radius:8px;padding:20px 28px;margin-bottom:18px;box-shadow:0 2px 8px #000a;">
                    <div style="font-weight:bold;font-size:1.1em;color:#90caf9;margin-bottom:8px;">Snapshot Log</div>
                    <div style="margin-bottom:10px;">
                        <label for="snapshotLogGrouping" style="color:#90caf9;">Group by</label>
                        <select id="snapshotLogGrouping">
                            <option value="snapshot">Per Snapshot</option>
                            <option value="day">Per Day</option>
                            <option value="week">Per Week</option>
                        </select>
                    </div>
                    <div id="snapshotLog">Loading...</div>
                </div>
            </div>
        </div>
        <script>
            function toggleSidebar() {
                document.getElementById('sidebar').classList.toggle('collapsed');
                document.getElementById('main-content').classList.toggle('collapsed');
            }
            // Unified sidebar server loader
            async function loadSidebarServers() {
                const res = await fetch('/api/servers');
                const servers = await res.json();
                const list = document.getElementById('serverList');
                list.innerHTML = '';
                let lastSelected = localStorage.getItem('nighty_last_server');
                let found = false;
                servers.forEach((srv, idx) => {
                    const li = document.createElement('li');
                    li.innerHTML = `<span class='server-name'>${srv.name || srv.id}</span>`;
                    li.onclick = () => {
                        localStorage.setItem('nighty_last_server', srv.id);
                        window.location.href = '/dashboard?guild_id=' + encodeURIComponent(srv.id);
                    };
                    list.appendChild(li);
                    if ((lastSelected && srv.id === lastSelected) || (!lastSelected && idx === 0)) {
                        li.classList.add('active');
                        found = true;
                    }
                });
                if (!found && servers.length > 0) {
                    list.firstChild.classList.add('active');
                }
            }
            // Call on every page
            loadSidebarServers();

            // Helper to get query param
            function getQueryParam(name) {
                const url = new URL(window.location.href);
                return url.searchParams.get(name);
            }
            const selectedGuildId = getQueryParam('guild_id');
            let selectedGuildName = null;

            // Update the dashboard title with the server name if available
            async function updateDashboardTitle() {
                if (selectedGuildId) {
                    // Try to get the server name from the sidebar list
                    const res = await fetch('/api/servers');
                    const servers = await res.json();
                    const server = servers.find(s => s.id === selectedGuildId);
                    selectedGuildName = server ? server.name : selectedGuildId;
                    document.querySelector('h1').textContent = `Analytics Overview - ${selectedGuildName}`;
                } else {
                    document.querySelector('h1').textContent = 'Analytics Overview';
                }
            }
            updateDashboardTitle();

            async function loadStats() {
                // Add guild_id to API calls if present
                const gid = selectedGuildId ? `?guild_id=${encodeURIComponent(selectedGuildId)}` : '';
                // Total Snapshots
                const snapRes = await fetch('/api/total_snapshots' + gid);
                const snapData = await snapRes.json();
                document.getElementById('totalSnapshots').textContent = snapData.count.toLocaleString();
                // Unique Members
                const uniqRes = await fetch('/api/user_count' + gid);
                const uniqData = await uniqRes.json();
                document.getElementById('uniqueMembers').textContent = uniqData.count.toLocaleString();
                // Total Memberships
                const memRes = await fetch('/api/membership_count' + gid);
                const memData = await memRes.json();
                document.getElementById('totalMemberships').textContent = memData.count.toLocaleString();
            }
            loadStats();

            async function loadSnapshots24hChart() {
                const gid = selectedGuildId ? `?guild_id=${encodeURIComponent(selectedGuildId)}` : '';
                const res = await fetch('/api/snapshots_24h' + gid);
                const data = await res.json();
                const ctx = document.getElementById('snapshots24hChart').getContext('2d');
                new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: data.hours,
                        datasets: [{
                            label: 'Snapshots',
                            data: data.counts,
                            backgroundColor: '#90caf9',
                        }]
                    },
                    options: {
                        plugins: { legend: { display: false }, tooltip: { enabled: true } },
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            x: { title: { display: true, text: 'Hour (UTC)', color: '#90caf9' }, ticks: { color: '#e0e0e0' }, grid: { color: '#333' } },
                            y: { title: { display: true, text: 'Snapshots', color: '#90caf9' }, ticks: { color: '#e0e0e0' }, grid: { color: '#333' }, beginAtZero: true }
                        },
                        interaction: { mode: 'nearest', intersect: false },
                        hover: { mode: 'nearest', intersect: false }
                    }
                });
            }
            loadSnapshots24hChart();

            let membersOverTimeChartInstance = null;
            async function loadMembersOverTimeChart(days = 7) {
                let url = '/api/members_over_time';
                const params = [];
                if (days !== 'all') params.push(`days=${days}`);
                if (selectedGuildId) params.push(`guild_id=${encodeURIComponent(selectedGuildId)}`);
                if (params.length) url += '?' + params.join('&');
                const res = await fetch(url);
                const data = await res.json();
                const ctx = document.getElementById('membersOverTimeChart').getContext('2d');
                if (membersOverTimeChartInstance) membersOverTimeChartInstance.destroy();
                // If no data, show a message and don't render chart
                if (!data.dates || !data.counts || data.dates.length === 0 || data.counts.length === 0) {
                    ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
                    ctx.font = '16px Segoe UI, Arial, sans-serif';
                    ctx.fillStyle = '#90caf9';
                    ctx.fillText('No member data available for this server.', 20, 40);
                    return;
                }
                membersOverTimeChartInstance = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: data.dates,
                        datasets: [{
                            label: 'Unique Members',
                            data: data.counts,
                            borderColor: '#90caf9',
                            backgroundColor: 'rgba(144,202,249,0.1)',
                            fill: true,
                            tension: 0.3
                        }]
                    },
                    options: {
                        plugins: { legend: { display: false }, tooltip: { enabled: true } },
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            x: { title: { display: true, text: 'Date', color: '#90caf9' }, ticks: { color: '#e0e0e0' }, grid: { color: '#333' } },
                            y: { title: { display: true, text: 'Unique Members', color: '#90caf9' }, ticks: { color: '#e0e0e0' }, grid: { color: '#333' }, beginAtZero: true }
                        },
                        interaction: { mode: 'nearest', intersect: false },
                        hover: { mode: 'nearest', intersect: false }
                    }
                });
            }
            document.getElementById('membersDaysSelect').addEventListener('change', function() {
                const val = this.value === 'all' ? 'all' : parseInt(this.value, 10);
                loadMembersOverTimeChart(val);
            });
            // Initial load
            loadMembersOverTimeChart(7);

            function toggleDemographics() {
                const content = document.getElementById('demographicsContent');
                const icon = document.getElementById('demoCollapseIcon');
                if (content.style.display === 'none') {
                    content.style.display = 'block';
                    icon.textContent = '-';
                } else {
                    content.style.display = 'none';
                    icon.textContent = '+';
                }
            }

            async function loadDemographics() {
                if (!selectedGuildId) {
                    document.getElementById('demographicsSection').style.display = 'none';
                    return;
                }
                document.getElementById('demographicsSection').style.display = '';
                const res = await fetch(`/api/server/${selectedGuildId}/demographics`);
                const data = await res.json();
                let html = '';
                function memberList(title, members) {
                    return `<div style='margin-bottom:10px;'><b>${title}:</b><ul style='margin:6px 0 0 18px;padding:0;'>` +
                        members.map(m => `<li>${m.name} <span style='color:#aaa;font-size:0.95em;'>(${m.account_created || m.joined_at || ''})</span></li>`).join('') +
                        '</ul></div>';
                }
                html += memberList('Oldest Accounts', data.oldest_accounts || []);
                html += memberList('Newest Accounts', data.newest_accounts || []);
                html += memberList('Longest Members', data.longest_members || []);
                html += memberList('Newest Members', data.newest_members || []);
                document.getElementById('demographicsLists').innerHTML = html;
            }

            document.getElementById('snapshotLogGrouping').addEventListener('change', function() {
                loadSnapshotLog();
            });

            async function loadSnapshotLog() {
                if (!selectedGuildId) {
                    document.getElementById('snapshotLogSection').style.display = 'none';
                    return;
                }
                document.getElementById('snapshotLogSection').style.display = '';
                const grouping = document.getElementById('snapshotLogGrouping').value;
                const res = await fetch(`/api/server/${selectedGuildId}/snapshots?group=${grouping}`);
                const data = await res.json();
                if (!data.length) {
                    document.getElementById('snapshotLog').innerHTML = '<span style="color:#888">No snapshots available.</span>';
                    return;
                }
                let html = '';
                // Reverse the data for most recent first and fix delta calculation
                const reversed = [...data].reverse();
                for (let i = 0; i < reversed.length; i++) {
                    const snap = reversed[i];
                    const date = snap.timestamp ? snap.timestamp.split('T')[0] : '';
                    const count = snap.member_count;
                    let diff = '';
                    if (i < reversed.length - 1) {
                        const next = reversed[i + 1];
                        const delta = count - next.member_count;
                        if (delta > 0) diff = `<span style='color:#4caf50;'>+${delta} joined</span>`;
                        else if (delta < 0) diff = `<span style='color:#f44336;'>${delta} left</span>`;
                        else continue; // skip 'no change'
                    } else {
                        diff = `<span style='color:#aaa;'>initial snapshot</span>`;
                    }
                    html += `<div style='margin-bottom:4px;'><b>${date}</b>: ${diff} <span style='color:#aaa;font-size:0.95em;'>(${count} members)</span></div>`;
                }
                document.getElementById('snapshotLog').innerHTML = html;
            }

            // Load these sections when a server is selected
            if (selectedGuildId) {
                loadDemographics();
                loadSnapshotLog();
            }
            // Adjust member chart height to match snapshots taken chart
            document.addEventListener('DOMContentLoaded', function() {
                const memberChart = document.getElementById('membersOverTimeChart');
                const snapshotsChart = document.getElementById('snapshotsTakenChart');
                if (memberChart && snapshotsChart) {
                    memberChart.style.height = snapshotsChart.offsetHeight + 'px';
                }
            });

            async function loadServerStats() {
                if (!selectedGuildId) {
                    document.getElementById('serverStatsCard').style.display = 'none';
                    return;
                }
                const res = await fetch(`/api/server/${selectedGuildId}/stats`);
                const data = await res.json();
                document.getElementById('serverStatsCard').style.display = '';
                document.getElementById('peakMemberCount').textContent = data.peak_member_count?.toLocaleString() ?? '...';
                // Append (+/-) difference from current after the peak value
                if (typeof data.peak_member_count === 'number' && typeof data.current_member_count === 'number') {
                    const diff = data.current_member_count - data.peak_member_count;
                    let diffStr = '';
                    if (diff !== 0) {
                        diffStr = ` (${diff > 0 ? '+' : ''}${diff.toLocaleString()})`;
                    } else {
                        diffStr = ' (0)';
                    }
                    document.getElementById('peakMemberCount').textContent += diffStr;
                }
                document.getElementById('peakMemberDate').textContent = data.peak_member_date ? `(at ${data.peak_member_date} UTC)` : '';
                document.getElementById('currentMemberCount').textContent = data.current_member_count?.toLocaleString() ?? '...';
                document.getElementById('changeSinceFirst').textContent = data.change_since_first ?? '...';
                document.getElementById('lastSnapshot').textContent = data.last_snapshot ? `${data.last_snapshot} UTC` : '...';
                document.getElementById('timeSinceLast').textContent = data.time_since_last ?? '...';
                document.getElementById('totalSnapshots').textContent = data.total_snapshots?.toLocaleString() ?? '...';
                document.getElementById('boosterCount').textContent = data.current_boosters?.toLocaleString() ?? '...';
            }
            loadServerStats();

            let totalMemberCountChart = null;
            async function loadTotalMemberCountChart(days = 7) {
                let url, labelKey, dataKey;
                if (days == 1) {
                    url = '/api/tracked_members_over_time_hourly';
                    labelKey = 'hours';
                    dataKey = 'counts';
                } else {
                    url = '/api/tracked_members_over_time?days=' + days;
                    labelKey = 'dates';
                    dataKey = 'counts';
                }
                const res = await fetch(url);
                const data = await res.json();
                const ctx = document.getElementById('totalMemberCountChart').getContext('2d');
                if (totalMemberCountChart) totalMemberCountChart.destroy();
                // Format labels: for 1d, show only hour (HH:mm); for others, keep as is
                let labels = data[labelKey];
                if (days == 1) {
                    labels = labels.map(ts => ts.slice(11, 16)); // 'HH:mm'
                }
                totalMemberCountChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: 'Tracked Members',
                            data: data[dataKey],
                            borderColor: '#90caf9',
                            backgroundColor: 'rgba(144,202,249,0.1)',
                            fill: true,
                            tension: 0.3
                        }]
                    },
                    options: {
                        plugins: { legend: { display: false }, tooltip: { enabled: true } },
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            x: { title: { display: true, text: days == 1 ? 'Hour' : 'Date', color: '#90caf9' }, ticks: { color: '#e0e0e0' }, grid: { color: '#333' } },
                            y: {
                                title: { display: true, text: 'Tracked Members', color: '#90caf9' },
                                ticks: {
                                    color: '#e0e0e0',
                                    stepSize: 1,
                                    callback: function(value) { return Number(value).toLocaleString(); },
                                    precision: 0
                                },
                                grid: { color: '#333' },
                                beginAtZero: true
                            }
                        },
                        interaction: { mode: 'nearest', intersect: false },
                        hover: { mode: 'nearest', intersect: false }
                    }
                });
            }
            document.getElementById('memberCountRange').addEventListener('change', function() {
                loadTotalMemberCountChart(this.value);
            });
            // Initial load
            loadTotalMemberCountChart(7);
        </script>
    </body>
    </html>
    ''')

@app.route('/api/user_history')
def user_history():
    member_id = request.args.get('member_id')
    if not member_id:
        return jsonify({'error': 'Missing member_id'}), 400
    db = get_db()
    # Get all demographics rows for this member
    rows = db.execute('SELECT * FROM demographics WHERE member_id = ? ORDER BY joined_at', (member_id,)).fetchall()
    # Get all snapshots this member was present in (if you track this)
    # For now, just return demographics rows
    return jsonify([dict(row) for row in rows])

@app.route('/api/server_configs')
def get_server_configs():
    try:
        db = get_db()
        # Ensure all servers with snapshots have config entries
        servers_with_snapshots = db.execute('''
            SELECT DISTINCT guild_id, guild_name 
            FROM snapshots 
            WHERE guild_name IS NOT NULL
        ''').fetchall()
        for server in servers_with_snapshots:
            existing = db.execute('SELECT guild_id FROM server_config WHERE guild_id = ?', (server['guild_id'],)).fetchone()
            if not existing:
                db.execute('''
                    INSERT INTO server_config (guild_id, auto_snapshot, last_auto_snapshot, 
                                            first_snapshot_date, chart_style, snapshot_retention_days, 
                                            auto_snapshot_interval_hours)
                    VALUES (?, 0, NULL, NULL, 'emoji', 90, 20)
                ''', (server['guild_id'],))
        # Get first snapshot date for each server
        first_snapshots = db.execute('''
            SELECT guild_id, MIN(timestamp) as first_snapshot
            FROM snapshots 
            GROUP BY guild_id
        ''').fetchall()
        for snapshot in first_snapshots:
            db.execute('''
                UPDATE server_config 
                SET first_snapshot_date = ? 
                WHERE guild_id = ? AND first_snapshot_date IS NULL
            ''', (snapshot['first_snapshot'], snapshot['guild_id']))
        db.commit()
        # Now get all configs with first_snapshot_date
        configs = db.execute('''
            SELECT guild_id, auto_snapshot, last_auto_snapshot, 
                first_snapshot_date, snapshot_retention_days, 
                auto_snapshot_interval_hours
            FROM server_config 
            WHERE first_snapshot_date IS NOT NULL
            ORDER BY guild_id
        ''').fetchall()
        
        # Get the most recent snapshot for each server
        latest_snapshots = db.execute('''
            SELECT guild_id, MAX(timestamp) as last_snapshot
            FROM snapshots 
            GROUP BY guild_id
        ''').fetchall()
        
        # Create a lookup for latest snapshots
        latest_snapshot_lookup = {row['guild_id']: row['last_snapshot'] for row in latest_snapshots}
        
        # Get server names
        server_names = {}
        for row in db.execute('SELECT guild_id, guild_name FROM snapshots WHERE guild_name IS NOT NULL GROUP BY guild_id').fetchall():
            server_names[row['guild_id']] = row['guild_name']
        
        result = []
        for row in configs:
            result.append({
                'guild_id': row['guild_id'],
                'guild_name': server_names.get(row['guild_id'], f"Server {row['guild_id']}"),
                'auto_snapshot': bool(row['auto_snapshot']),
                'last_auto_snapshot': row['last_auto_snapshot'],
                'last_snapshot': latest_snapshot_lookup.get(row['guild_id']),
                'first_snapshot_date': row['first_snapshot_date'],
                'snapshot_retention_days': row['snapshot_retention_days'],
                'auto_snapshot_interval_hours': row['auto_snapshot_interval_hours']
            })
        return jsonify(result)
    except Exception as e:
        print(f"Error in get_server_configs: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/update_config', methods=['POST'])
def update_server_config():
    """Update server configuration settings"""
    try:
        data = request.get_json()
        guild_id = data.get('guild_id')
        field = data.get('field')
        value = data.get('value')
        
        if not guild_id or not field:
            return jsonify({'error': 'Missing guild_id or field'}), 400
        
        db = get_db()
        
        # Validate field name to prevent SQL injection
        allowed_fields = {
            'auto_snapshot', 'auto_snapshot_interval_hours', 
            'snapshot_retention_days', 'chart_style'
        }
        
        if field not in allowed_fields:
            return jsonify({'error': f'Invalid field: {field}'}), 400
        
        # Convert boolean for auto_snapshot
        if field == 'auto_snapshot':
            value = 1 if value else 0
        elif field in ['auto_snapshot_interval_hours', 'snapshot_retention_days']:
            try:
                value = int(value)
                if value <= 0:
                    return jsonify({'error': f'{field} must be positive'}), 400
            except (ValueError, TypeError):
                return jsonify({'error': f'{field} must be a number'}), 400
        
        # Update the configuration
        db.execute(f'UPDATE server_config SET {field} = ? WHERE guild_id = ?', (value, guild_id))
        db.commit()
        
        # Log the update via webhook
        field_name = field.replace('_', ' ').title()
        embed = {
            "title": "Configuration Updated",
            "description": f"Updated {field_name} for server {guild_id}",
            "fields": [
                {"name": "Field", "value": field_name, "inline": True},
                {"name": "Value", "value": str(value), "inline": True},
                {"name": "Server ID", "value": str(guild_id), "inline": True}
            ],
            "color": 0x90caf9
        }
        send_webhook_log("", embed=embed)
        
        return jsonify({'success': True})
        
    except Exception as e:
        print(f"Error updating config: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/take_snapshot/<guild_id>', methods=['POST'])
def take_manual_snapshot(guild_id):
    """Take a manual snapshot for a specific server"""
    try:
        import requests
        import os
        from datetime import datetime, timezone
        
        # Get the API token from environment variable
        api_token = os.environ.get('NIGHTY_API_TOKEN', 'default_token_change_me')
        
        # Prepare the request to the NightyScript micro-API
        api_url = f'{NIGHTY_API_BASE_URL}/take_snapshot'
        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }
        data = {
            'guild_id': str(guild_id),
            'token': api_token,
            'manual': True
        }
        
        print(f"[Analytics] Making request to {api_url} for guild {guild_id}")
        
        # Make the request to the NightyScript micro-API
        response = requests.post(api_url, headers=headers, json=data, timeout=30)
        
        print(f"[Analytics] Response status: {response.status_code}")
        print(f"[Analytics] Response content: {response.text[:200]}...")
        
        if response.status_code == 200:
            try:
                result = response.json()
            except json.JSONDecodeError as e:
                print(f"[Analytics] JSON decode error: {e}")
                print(f"[Analytics] Full response content: {response.text}")
                return jsonify({
                    'success': False,
                    'error': f'Invalid JSON response from micro-API: {response.text[:100]}'
                }), 500
            
            # Update last_snapshot in database (do NOT update last_auto_snapshot for manual)
            db = get_db()
            now = datetime.now(timezone.utc)
            db.execute(
                'UPDATE server_config SET last_auto_snapshot = ? WHERE guild_id = ?',
                (now.isoformat(), guild_id)
            )
            db.commit()
            
            # Webhook log on success
            # guild_name = result.get('guild_name', f'Server {guild_id}')
            # member_count = result.get('member_count', 0)
            # timestamp = now.strftime('%Y-%m-%d %H:%M:%S UTC')
            # embed = {
            #     "title": "Manual Snapshot Taken",
            #     "description": f"Manual snapshot taken for {guild_name} at {timestamp}",
            #     "fields": [
            #         {"name": "Member Count", "value": str(member_count), "inline": True},
            #         {"name": "Type", "value": "Manual", "inline": True},
            #         {"name": "Server ID", "value": str(guild_id), "inline": True}
            #     ],
            #     "color": 0x4caf50
            # }
            # send_webhook_log("", embed=embed)
            
            return jsonify({
                'success': True,
                'message': f'Snapshot taken successfully for server {guild_id}',
                'member_count': result.get('member_count', 0),
                'guild_name': result.get('guild_name', f'Server {guild_id}')
            })
        else:
            # Try to parse error response as JSON, but handle non-JSON responses
            try:
                error_data = response.json()
                error_message = error_data.get('error', f'HTTP {response.status_code}')
            except json.JSONDecodeError:
                error_message = f'HTTP {response.status_code}: {response.text[:100]}'
            
            return jsonify({
                'success': False,
                'error': error_message
            }), response.status_code
            
    except requests.exceptions.ConnectionError:
        return jsonify({
            'success': False,
            'error': 'Cannot connect to NightyScript micro-API. Make sure the NightyScript is running.'
        }), 503
    except requests.exceptions.Timeout:
        return jsonify({
            'success': False,
            'error': 'Request to NightyScript micro-API timed out.'
        }), 504
    except Exception as e:
        print(f"[Analytics] Unexpected error in take_manual_snapshot: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/auto_snapshot_notification', methods=['POST'])
def receive_auto_snapshot_notification():
    """Receive auto snapshot notifications from the NightyScript micro-API"""
    try:
        # Verify the request is from the micro-API
        auth_header = request.headers.get('Authorization')
        api_token = os.environ.get('NIGHTY_API_TOKEN', 'default_token_change_me')
        
        if not auth_header or auth_header != f'Bearer {api_token}':
            return jsonify({'error': 'Unauthorized'}), 401
        
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        guild_id = data.get('guild_id')
        guild_name = data.get('guild_name', f'Server {guild_id}')
        member_count = data.get('member_count', 0)
        timestamp = data.get('timestamp')
        is_auto = data.get('is_auto', True)
        
        if not guild_id:
            return jsonify({'error': 'Missing guild_id'}), 400
        
        # Update last_auto_snapshot in database if this was an auto snapshot
        if is_auto:
            db = get_db()
            db.execute(
                'UPDATE server_config SET last_auto_snapshot = ?, last_snapshot = ? WHERE guild_id = ?',
                (timestamp, timestamp, guild_id)
            )
            db.commit()
        
        # Format timestamp for display
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                # Format to match other webhook messages: "2025-06-26 22:28:14 UTC"
                display_time = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            except:
                display_time = timestamp
        else:
            display_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Webhook log
        snapshot_type = "Auto" if is_auto else "Manual"
        color = 0x2196f3 if is_auto else 0x4caf50  # Blue for auto, green for manual
        
        embed = {
            "title": f"{snapshot_type} Snapshot Taken",
            "description": f"{snapshot_type.lower().capitalize()} snapshot taken for {guild_name} at {display_time}",
            "fields": [
                {"name": "Member Count", "value": str(member_count), "inline": True},
                {"name": "Type", "value": snapshot_type, "inline": True},
                {"name": "Server ID", "value": str(guild_id), "inline": True}
            ],
            "color": color
        }
        send_webhook_log("", embed=embed)
        
        print(f"[Analytics] Received {snapshot_type.lower()} snapshot notification for {guild_name} ({guild_id})")
        
        return jsonify({'success': True, 'message': f'{snapshot_type} snapshot logged successfully'})
        
    except Exception as e:
        print(f"[Analytics] Error in auto_snapshot_notification: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/test_auto_snapshot/<guild_id>', methods=['POST'])
def test_auto_snapshot(guild_id):
    """Test endpoint to simulate an auto snapshot notification"""
    try:
        from datetime import datetime, timezone
        
        # Create a test auto snapshot notification
        test_data = {
            'guild_id': str(guild_id),
            'guild_name': f'Test Server {guild_id}',
            'member_count': 1234,  # Test member count
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'is_auto': True
        }
        
        # Send the notification to our own endpoint
        response = requests.post(
            f'http://127.0.0.1:5000/api/auto_snapshot_notification',
            headers={'Authorization': f'Bearer {os.environ.get("NIGHTY_API_TOKEN", "default_token_change_me")}'},
            json=test_data,
            timeout=10
        )
        
        if response.status_code == 200:
            return jsonify({
                'success': True,
                'message': 'Test auto snapshot notification sent successfully',
                'test_data': test_data
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Failed to send test notification: {response.text}'
            }), response.status_code
            
    except Exception as e:
        print(f"[Analytics] Error in test_auto_snapshot: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/snapshot_all', methods=['POST'])
def snapshot_all():
    import requests
    import os
    import time
    try:
        db = get_db()
        api_token = os.environ.get('NIGHTY_API_TOKEN', 'default_token_change_me')
        api_url = os.environ.get('NIGHTY_API_BASE_URL', 'http://127.0.0.1:5500') + '/take_snapshot'
        servers = db.execute('SELECT DISTINCT guild_id FROM server_config').fetchall()
        if not servers:
            servers = db.execute('SELECT DISTINCT guild_id FROM snapshots').fetchall()
        count = 0
        errors = []
        failed_servers = []
        for row in servers:
            guild_id = row['guild_id'] if isinstance(row, dict) else row[0]
            try:
                res = requests.post(api_url, headers={
                    'Authorization': f'Bearer {api_token}',
                    'Content-Type': 'application/json'
                }, json={
                    'guild_id': str(guild_id),
                    'token': api_token,
                    'manual': True
                }, timeout=30)
                if res.status_code == 200:
                    count += 1
                else:
                    errors.append(f'{guild_id}: {res.text[:100]}')
                    failed_servers.append(str(guild_id))
            except Exception as e:
                errors.append(f'{guild_id}: {str(e)}')
                failed_servers.append(str(guild_id))
            time.sleep(5)
        # Send summary webhook
        summary = f"Snapshot All Complete\nSuccess: {count}\nFailed: {len(failed_servers)}"
        if failed_servers:
            summary += f"\nFailed Servers: {', '.join(failed_servers)}"
        embed = {
            "title": "Snapshot All Summary",
            "description": summary,
            "color": 0x90caf9 if not failed_servers else 0xf44336
        }
        send_webhook_log("", embed=embed)
        if errors:
            return {'success': False, 'error': f'Errors for some servers: {errors}', 'count': count}
        return {'success': True, 'count': count}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.route('/api/tracked_members_over_time')
def tracked_members_over_time():
    db = get_db()
    import datetime

    days = request.args.get('days', default=None, type=int)
    today = datetime.date.today()
    if days is not None:
        date_list = [(today - datetime.timedelta(days=i)).isoformat() for i in range(days-1, -1, -1)]
    else:
        # Use all days from earliest to today
        rows = db.execute("SELECT timestamp FROM demographics").fetchall()
        timestamps = [row['timestamp'][:10] for row in rows if row['timestamp'] and len(row['timestamp']) >= 10]
        if timestamps:
            first_day = min(timestamps)
        else:
            first_day = today.isoformat()
        d = datetime.date.fromisoformat(first_day)
        date_list = []
        while d <= today:
            date_list.append(d.isoformat())
            d += datetime.timedelta(days=1)

    # For each day, count users with timestamp <= that day
    result_counts = []
    for d in date_list:
        count = db.execute(
            "SELECT COUNT(*) FROM demographics WHERE substr(timestamp, 1, 10) <= ?", (d,)
        ).fetchone()[0]
        result_counts.append(count)

    return jsonify({'dates': date_list, 'counts': result_counts})

@app.route('/api/fetch_all', methods=['POST'])
def fetch_all_members():
    import requests
    import os
    import time
    try:
        db = get_db()
        api_token = os.environ.get('NIGHTY_API_TOKEN', 'default_token_change_me')
        api_url = os.environ.get('NIGHTY_API_BASE_URL', 'http://127.0.0.1:5500') + '/fetch_members'
        servers = db.execute('SELECT DISTINCT guild_id FROM server_config').fetchall()
        if not servers:
            servers = db.execute('SELECT DISTINCT guild_id FROM snapshots').fetchall()
        count = 0
        errors = []
        failed_servers = []
        for row in servers:
            guild_id = row['guild_id'] if isinstance(row, dict) else row[0]
            try:
                res = requests.post(api_url, headers={
                    'Authorization': f'Bearer {api_token}',
                    'Content-Type': 'application/json'
                }, json={
                    'guild_id': str(guild_id),
                    'token': api_token
                }, timeout=30)
                if res.status_code == 200:
                    count += 1
                else:
                    errors.append(f'{guild_id}: {res.text[:100]}')
                    failed_servers.append(str(guild_id))
            except Exception as e:
                errors.append(f'{guild_id}: {str(e)}')
                failed_servers.append(str(guild_id))
            time.sleep(10)
        # Send summary webhook
        summary = f"Fetch All Complete\nSuccess: {count}\nFailed: {len(failed_servers)}"
        if failed_servers:
            summary += f"\nFailed Servers: {', '.join(failed_servers)}"
        embed = {
            "title": "Fetch All Summary",
            "description": summary,
            "color": 0x4caf50 if not failed_servers else 0xf44336
        }
        send_webhook_log("", embed=embed)
        if errors:
            return {'success': False, 'error': f'Errors for some servers: {errors}', 'count': count}
        return {'success': True, 'count': count}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.route('/api/validate_database', methods=['POST'])
def api_validate_database():
    """API endpoint to validate and repair database schema"""
    try:
        with app.app_context():
            is_valid = validate_and_repair_database()
            return jsonify({
                'success': True,
                'is_valid': is_valid,
                'message': 'Database validation completed'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def remove_emojis_from_text(text):
    # Emoji unicode ranges (broad, not perfect)
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002700-\U000027BF"  # Dingbats
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U00002600-\U000026FF"  # Misc symbols
        "\U0001F700-\U0001F77F"  # Alchemical Symbols
        "\U000024C2-\U0001F251"  # Enclosed characters
        "]+",
        flags=re.UNICODE
    )
    return emoji_pattern.sub(r'', text)

# Read the file
with open(__file__, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Remove emojis from each line
new_lines = [remove_emojis_from_text(line) for line in lines]

# Write back to the file
with open(__file__, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

@app.route('/api/tracked_members_over_time_hourly')
def tracked_members_over_time_hourly():
    db = get_db()
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc)
    # List of 24 datetimes, one for each hour (on the hour)
    hours = [(now - datetime.timedelta(hours=i)).replace(minute=0, second=0, microsecond=0) for i in range(23, -1, -1)]
    hour_labels = [h.strftime('%Y-%m-%d %H:00') for h in hours]
    counts = []
    prev_count = 0
    for h in hours:
        # Find the latest snapshot at or before this hour
        row = db.execute(
            "SELECT member_count FROM snapshots WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
            (h.isoformat(),)
        ).fetchone()
        if row is not None:
            prev_count = row[0]
        counts.append(prev_count)
    return jsonify({'hours': hour_labels, 'counts': counts})

if __name__ == '__main__':
    with app.app_context():
        init_database()
    app.run(debug=True)
