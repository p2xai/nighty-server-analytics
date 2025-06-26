@nightyScript(
    name="Server Analytics - SQLite Edition",
    author="thedorekaczynski",
    description="Discord server member tracking and analytics system with growth trends (SQLite backend)",
    usage="""
<p>analytics snapshot - Take a server snapshot
<p>analytics report - Generate a server analytics report
<p>analytics clear - Clear analytics data
<p>analytics status - Show analytics collection status
<p>analytics members - Show member count history graph
<p>analytics trend - Show member growth trend analysis
<p>analytics compare <days> - Compare server stats between two time periods
<p>analytics export - Export analytics data to a text file
<p>analytics auto <on/off> - Toggle automatic daily snapshots
<p>analytics retention [days] - Set snapshot data retention period
<p>analytics interval [hours] - Set auto-snapshot interval
<p>a <subcommand> - Shorthand for analytics command (same functionality)
<p>a ss - Short command for taking a snapshot
<p>a timezone <zone> - Set your preferred timezone (EST, PST, etc.)
<p>analytics demographics         - Show a summary of tracked account creation and join dates for this server
<p>analytics demographics list    - List all servers currently tracked for demographics
<p>analytics demographics remove <server_id> - Remove a server from demographics tracking
<p>analytics migrate - Migrate all analytics data from JSON to SQLite DB
<p>analytics holylogger - Auto-snapshot and fetch members for all unmonitored servers
<p>analytics boosters - List all current server boosters
"""
)
def server_analytics():
    """
    SERVER ANALYTICS SCRIPT (DB Edition)
    ---------------------
    Advanced Discord server member tracking and analytics with growth prediction, now using SQLite for all data storage.
    
    COMMANDS:
    <p>analytics snapshot     - Take an immediate snapshot
    <p>analytics report      - Generate detailed analytics report
    <p>analytics clear      - Clear analytics data
    <p>analytics status     - Show analytics collection status
    <p>analytics members    - Show member count history graph
    <p>analytics trend      - Show member growth trend analysis
    <p>analytics compare [days] - Compare server stats between two time periods
    <p>analytics export     - Export analytics data to a text file
    <p>analytics auto [on/off] - Manage automatic snapshots
    <p>analytics retention [days] - Set data retention period
    <p>analytics interval [hours] - Set auto snapshot interval
    <p>a ss                 - Quick snapshot
    <p>a timezone <zone>    - Set timezone
    <p>analytics demographics` - Show demographics summary
    <p>analytics demographics list` - List tracked servers
    <p>analytics demographics remove <server_id>` - Remove server from demographics tracking
    <p>analytics migrate` - Migrate all analytics data from JSON to SQLite DB
    <p>analytics holylogger` - Auto-snapshot and fetch members for all unmonitored servers
    <p>analytics boosters` - List all current server boosters

    EXAMPLES:
    <p>analytics snapshot
    <p>analytics report
    <p>analytics migrate
    <p>analytics holylogger
    
    NOTES:
    - All analytics data is now stored in analytics_test.db (SQLite)
    - Run <p>analytics migrate to import your old JSON data
    - After migration, all commands use the database only
    - The analytics compile command and HTML export are removed
    - Follows NightyScript best practices (see prompt.md)
    """
    import sqlite3
    import os
    import json
    import asyncio
    from datetime import datetime, timedelta, timezone
    import random
    import math
    from pathlib import Path
    import re
    from collections import defaultdict
    import time
    import discord
    import aiohttp
    from aiohttp import web
    import os
    import shutil
    import traceback

    # Constants
    DEFAULT_AUTO_SNAPSHOT_INTERVAL_HOURS = 20
    DATA_RETENTION_DAYS = 90
    
    AUTO_SNAPSHOT_CONFIG_KEY = "server_analytics_auto_snapshot"
    LAST_AUTO_SNAPSHOT_KEY = "server_analytics_last_auto"
    
    # Timezone configuration
    TIMEZONE_CONFIG_KEY = "server_analytics_timezone"
    DEFAULT_TIMEZONE = "UTC"
    
    # API configuration
    API_TOKEN = os.environ.get('NIGHTY_API_TOKEN', 'default_token_change_me')
    API_HOST = '127.0.0.1'
    API_PORT = 5500
    
    # Timezone offsets (in hours from UTC)
    TIMEZONE_OFFSETS = {
    "UTC": 0,
    "EST": -4,  # Eastern Standard Time (adjusted to match current EDT)
    "EDT": -4,  # Eastern Daylight Time
    "CST": -6,  # Central Standard Time
    "CDT": -5,  # Central Daylight Time
    "MST": -7,  # Mountain Standard Time
    "MDT": -6,  # Mountain Daylight Time
    "PST": -8,  # Pacific Standard Time
    "PDT": -7,  # Pacific Daylight Time
    "AKST": -9, # Alaska Standard Time
    "AKDT": -8, # Alaska Daylight Time
    "HST": -10, # Hawaii Standard Time
    "AEST": 10, # Australian Eastern Standard Time
    "AEDT": 11, # Australian Eastern Daylight Time
    "GMT": 0,   # Greenwich Mean Time
    "BST": 1,   # British Summer Time
    "CET": 1,   # Central European Time
    "CEST": 2,  # Central European Summer Time
    "JST": 9,   # Japan Standard Time
    "IST": 5.5, # India Standard Time (note: "IST" can also mean Irish Standard Time)
    }

    # Ensure base directory exists
    BASE_DIR = Path(getScriptsPath()) / "json" / "server_member_tracking"
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    DEMO_TRACKED_FILE = Path(getScriptsPath()) / "json" / "demographics_servers.json"
    def load_tracked_servers():
        try:
            with open(DEMO_TRACKED_FILE, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []
    def save_tracked_servers(servers):
        with open(DEMO_TRACKED_FILE, "w") as f:
            json.dump(servers, f, indent=4)
    def add_tracked_server(guild_id):
        servers = load_tracked_servers()
        if str(guild_id) not in servers:
            servers.append(str(guild_id))
            save_tracked_servers(servers)
    def remove_tracked_server(guild_id):
        servers = load_tracked_servers()
        if str(guild_id) in servers:
            servers.remove(str(guild_id))
            save_tracked_servers(servers)

    def get_server_dir(guild_id):
        """Get the directory for a specific server's data"""
        server_dir = BASE_DIR / str(guild_id)
        server_dir.mkdir(parents=True, exist_ok=True)
        return server_dir

    def get_server_files(guild_id):
        """Get file paths for a specific server's data"""
        server_dir = get_server_dir(guild_id)
        return {
            'snapshots': server_dir / "member_snapshots.json",
            'config': server_dir / "analytics_config.json"
        }
        
    def get_timezone():
        """Get the configured timezone or default to UTC"""
        return getConfigData().get(TIMEZONE_CONFIG_KEY, DEFAULT_TIMEZONE)
        
    def get_timezone_offset():
        """Get the timezone offset in hours from UTC"""
        timezone = get_timezone()
        return TIMEZONE_OFFSETS.get(timezone, 0)
        
    def format_time_in_timezone(utc_time, format_str="%b %d, %I:%M %p"):
        """Format a UTC time in the configured timezone"""
        timezone = get_timezone()
        offset = get_timezone_offset()
        
        # Apply the offset
        local_time = utc_time + timedelta(hours=offset)
        
        # Format the time
        formatted_time = local_time.strftime(format_str)
        
        # Add the timezone abbreviation
        return f"{formatted_time} {timezone}"
    
    # Server configuration management
    def load_server_config(guild_id):
        """Load server-specific configuration from database"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT auto_snapshot, last_auto_snapshot, first_snapshot_date, chart_style, snapshot_retention_days, auto_snapshot_interval_hours FROM server_config WHERE guild_id = ?", (str(guild_id),))
            row = c.fetchone()
            conn.close()
            
            if row:
                return {
                    "auto_snapshot": bool(row[0]),
                    "last_auto_snapshot": row[1],
                    "first_snapshot_date": row[2],
                    "chart_style": row[3] or "emoji",
                    "snapshot_retention_days": row[4] or DATA_RETENTION_DAYS,
                    "auto_snapshot_interval_hours": row[5] or DEFAULT_AUTO_SNAPSHOT_INTERVAL_HOURS,
                }
            else:
                # Return default config if not found
                return {
                    "auto_snapshot": False,
                    "last_auto_snapshot": None,
                    "first_snapshot_date": None,
                    "chart_style": "emoji",
                    "snapshot_retention_days": DATA_RETENTION_DAYS,
                    "auto_snapshot_interval_hours": DEFAULT_AUTO_SNAPSHOT_INTERVAL_HOURS,
                }
        except Exception as e:
            print(f"Error loading server config for {guild_id}: {e}", type_="ERROR")
            return {
                "auto_snapshot": False,
                "last_auto_snapshot": None,
                "first_snapshot_date": None,
                "chart_style": "emoji",
                "snapshot_retention_days": DATA_RETENTION_DAYS,
                "auto_snapshot_interval_hours": DEFAULT_AUTO_SNAPSHOT_INTERVAL_HOURS,
            }
        
    def update_server_config(guild_id, key, value):
        """Update a specific server configuration value in database"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Map config keys to database columns
            column_map = {
                "auto_snapshot": "auto_snapshot",
                "last_auto_snapshot": "last_auto_snapshot", 
                "first_snapshot_date": "first_snapshot_date",
                "chart_style": "chart_style",
                "snapshot_retention_days": "snapshot_retention_days",
                "auto_snapshot_interval_hours": "auto_snapshot_interval_hours"
            }
            
            if key in column_map:
                column = column_map[key]
                c.execute(f"UPDATE server_config SET {column} = ? WHERE guild_id = ?", (value, str(guild_id)))
                conn.commit()
            
            conn.close()
            return load_server_config(guild_id)
        except Exception as e:
            print(f"Error updating server config for {guild_id}: {e}", type_="ERROR")
            return load_server_config(guild_id)
        
    def is_auto_snapshot_enabled(guild_id):
        """Check if automatic snapshots are enabled for this server"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT auto_snapshot FROM server_config WHERE guild_id = ?", (str(guild_id),))
            row = c.fetchone()
            conn.close()
            return bool(row[0]) if row and row[0] is not None else False
        except Exception as e:
            print(f"Error checking auto_snapshot status for {guild_id}: {e}", type_="ERROR")
            return False
        
    def should_take_auto_snapshot(guild_id):
        """Return True if enough time has passed for an automatic snapshot."""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT last_auto_snapshot, auto_snapshot_interval_hours FROM server_config WHERE guild_id = ?", (str(guild_id),))
            row = c.fetchone()
            conn.close()
            
            if not row or row[0] is None:
                return True
                
            last_snapshot = row[0]
            interval = row[1] if row[1] is not None else DEFAULT_AUTO_SNAPSHOT_INTERVAL_HOURS

            # Parse the last snapshot time and ensure it's timezone-aware
            last_time = datetime.fromisoformat(last_snapshot)
            if last_time.tzinfo is None:
                # If the datetime is naive, assume it's UTC
                last_time = last_time.replace(tzinfo=timezone.utc)
            
            now = datetime.now(timezone.utc)

            return (now - last_time).total_seconds() >= interval * 3600
        except Exception as e:
            print(f"Error checking auto_snapshot timing for {guild_id}: {e}", type_="ERROR")
            return False

    # Initialize data structures if they don't exist
    def initialize_data(guild_id):
        """Initialize database schema if needed"""
        create_schema()

    # Load data from JSON files
    def load_data(file_path):
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    # Save data to JSON files
    def save_data(file_path, data):
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)

    # Take a server snapshot
    async def take_snapshot(guild, is_auto=False):
        # Ensure database schema exists
        create_schema()
        
        # Get channel and role counts
        voice_channels = 0
        text_channels = 0
        categories = 0
        for channel in guild.channels:
            channel_type = str(channel.type).lower()
            if channel_type == "text":
                text_channels += 1
            elif channel_type == "voice":
                voice_channels += 1
            elif channel_type == "category":
                categories += 1
        
        # Create snapshot
        timestamp = datetime.now(timezone.utc)
        member_count = guild.member_count
        channel_count = len(guild.channels)
        role_count = len(guild.roles)
        bots = len([m for m in guild.members if m.bot])
        
        # Insert into SQLite database
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Insert snapshot
        c.execute("""
            INSERT INTO snapshots (guild_id, guild_name, timestamp, member_count, channel_count, text_channels, voice_channels, categories, role_count, bots, is_auto)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(guild.id),
            guild.name,
            timestamp.isoformat(),
            member_count,
            channel_count,
            text_channels,
            voice_channels,
            categories,
            role_count,
            bots,
            int(is_auto)
        ))
        
        # Get current auto_snapshot setting to preserve it
        c.execute("SELECT auto_snapshot, chart_style, snapshot_retention_days, auto_snapshot_interval_hours FROM server_config WHERE guild_id = ?", (str(guild.id),))
        config_row = c.fetchone()
        
        if config_row:
            current_auto_snapshot = config_row[0] if config_row[0] is not None else 0
            current_chart_style = config_row[1] if config_row[1] else "emoji"
            current_retention_days = config_row[2] if config_row[2] else DATA_RETENTION_DAYS
            current_interval_hours = config_row[3] if config_row[3] else DEFAULT_AUTO_SNAPSHOT_INTERVAL_HOURS
        else:
            current_auto_snapshot = 0
            current_chart_style = "emoji"
            current_retention_days = DATA_RETENTION_DAYS
            current_interval_hours = DEFAULT_AUTO_SNAPSHOT_INTERVAL_HOURS
        
        # Update or create server config - preserve existing auto_snapshot setting
        c.execute("""
            INSERT OR REPLACE INTO server_config (guild_id, auto_snapshot, last_auto_snapshot, first_snapshot_date, chart_style, snapshot_retention_days, auto_snapshot_interval_hours)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            str(guild.id),
            current_auto_snapshot,  # Preserve current auto_snapshot setting
            timestamp.isoformat() if is_auto else None,
            timestamp.isoformat(),  # first_snapshot_date
            current_chart_style,
            current_retention_days,
            current_interval_hours
        ))
        
        conn.commit()
        conn.close()
        
        # Add to tracked servers for demographics
        add_tracked_server(guild.id)
        
        return {
            "timestamp": timestamp.isoformat(),
            "member_count": member_count,
            "channel_count": channel_count,
            "role_count": role_count,
            "categories": categories,
            "text_channels": text_channels,
            "voice_channels": voice_channels,
            "bots": bots,
            "is_auto": is_auto
        }

    # Handle auto-snapshot functionality
    @bot.listen("on_message")
    async def auto_snapshot_handler(message):
        # Only trigger on chance to prevent excessive checks
        if random.random() > 0.05:  # Only run 5% of the time
            return
            
        # Skip if this is a DM or not in a guild
        if not message.guild:
            return
            
        # Check if auto-snapshots are enabled for this guild
        if not is_auto_snapshot_enabled(message.guild.id):
            return
            
        # Check if enough time has passed since last auto snapshot
        if not should_take_auto_snapshot(message.guild.id):
            return
            
        # Take the snapshot silently
        try:
            await take_snapshot(message.guild, is_auto=True)
            print(f"Auto-snapshot taken for {message.guild.name} (ID: {message.guild.id})", type_="INFO")
        except Exception as e:
            print(f"Error taking auto-snapshot: {str(e)}", type_="ERROR")

    # Analyze member growth trends
    def analyze_growth_trend(snapshots, days=7):
        """Analyze member growth trends and predict future growth"""
        if len(snapshots) < 2:
            return {
                "trend": "insufficient_data",
                "growth_rate_daily": 0,
                "prediction_7_days": None,
                "prediction_30_days": None,
                "confidence": "low"
            }
            
        # Sort snapshots by timestamp
        sorted_snapshots = sorted(snapshots, key=lambda x: x["timestamp"])
        
        # Get the most recent snapshot
        current = sorted_snapshots[-1]
        current_time = datetime.fromisoformat(current["timestamp"])
        current_count = current["member_count"]
        
        # Find a snapshot from approximately 'days' days ago
        target_time = current_time - timedelta(days=days)
        closest_snapshot = min(sorted_snapshots[:-1], 
                              key=lambda x: abs(datetime.fromisoformat(x["timestamp"]) - target_time))
        past_time = datetime.fromisoformat(closest_snapshot["timestamp"])
        past_count = closest_snapshot["member_count"]
        
        # Calculate time difference in days
        time_diff_days = (current_time - past_time).total_seconds() / (24 * 3600)
        
        # If time difference is too small, adjust analysis
        if time_diff_days < 1:
            time_diff_days = 1  # Minimum 1 day to avoid division issues
            
        # Calculate growth
        member_diff = current_count - past_count
        growth_rate_daily = member_diff / time_diff_days
        
        # Determine trend type
        if member_diff > 0:
            if growth_rate_daily >= 10:
                trend = "rapid_growth"
            elif growth_rate_daily >= 3:
                trend = "steady_growth"
            else:
                trend = "slow_growth"
        elif member_diff < 0:
            if growth_rate_daily <= -10:
                trend = "rapid_decline"
            elif growth_rate_daily <= -3:
                trend = "steady_decline"
            else:
                trend = "slow_decline"
        else:
            trend = "stable"
            
        # Make predictions
        prediction_7_days = round(current_count + (growth_rate_daily * 7))
        prediction_30_days = round(current_count + (growth_rate_daily * 30))
        
        # Determine confidence based on data points and consistency
        # More snapshots and consistent growth pattern = higher confidence
        if len(snapshots) >= 10:
            confidence = "high"
        elif len(snapshots) >= 5:
            confidence = "medium"
        else:
            confidence = "low"
            
        return {
            "trend": trend,
            "growth_rate_daily": growth_rate_daily,
            "growth_total": member_diff,
            "days_measured": round(time_diff_days, 1),
            "prediction_7_days": prediction_7_days,
            "prediction_30_days": prediction_30_days,
            "confidence": confidence
        }

    # Commands
    @bot.command(name="analytics", aliases=["a"], description="Server analytics commands")
    async def analytics_cmd(ctx, *, args: str = ""):
        # Store message for later deletion
        cmd_msg = ctx.message
        
        try:
            await cmd_msg.delete()
        except Exception as e:
            print(f"Error deleting command message: {str(e)}", type_="ERROR")
        
        # Robust subcommand parsing
        parts = args.strip().split(maxsplit=2)
        cmd = parts[0].lower() if parts else ""
        subcmd = parts[1].lower() if len(parts) > 1 else ""
        subarg = parts[2] if len(parts) > 2 else ""
        user_input = args.strip()
        
        # Shorthand aliases for commands
        aliases = {
            "ss": "snapshot",
            "rep": "report",
            "clr": "clear",
            "stat": "status",
            "mem": "members",
            "tr": "trend",
            "cmp": "compare",
            "exp": "export",
            "auto": "auto",
            "ret": "retention",
            "int": "interval",
            "tz": "timezone",
            "demo": "demographics",
            "holy": "holylogger",
            "boosters": "boosters"
        }
        if cmd in aliases:
            cmd = aliases[cmd]
        
        if cmd == "help" or cmd == "?":
            await ctx.send(""" **server analytics commands**

• `<p>analytics snapshot` (ss) - take a snapshot
• `<p>analytics report` (rep) - generate detailed report
• `<p>analytics clear` (clr) - clear data
• `<p>analytics status` (stat) - show collection status
• `<p>analytics members` (mem) - show recent member changes
• `<p>analytics trend` (tr) - show growth trend analysis
• `<p>analytics compare [days]` (cmp) - compare with previous period
• `<p>analytics export` (exp) - export data to text file
• `<p>analytics auto [on/off]` - manage automatic snapshots
• `<p>analytics retention [days]` (ret) - set data retention period
• `<p>analytics interval [hours]` (int) - set auto snapshot interval
• `<p>a <subcommand>` - shorthand for commands
• `<p>a ss` - quick snapshot
• `<p>a timezone <zone>` (tz) - set timezone
• `<p>analytics demographics` (demo) - show demographics summary
• `<p>analytics demographics list` - list tracked servers
• `<p>analytics demographics remove <server_id>` - remove server from demographics tracking
• `<p>analytics migrate` - migrate all analytics data from JSON to SQLite DB
• `<p>analytics holylogger` (holy) - auto-snapshot and fetch members for all unmonitored servers
• `<p>analytics boosters` - list all current server boosters
• `<p>analytics api start` - start the micro-API server manually
• `<p>analytics api status` - check if API server is running
• `<p>analytics api stop` - stop the API server
""")
            return
        
        if cmd == "snapshot":
            msg = await ctx.send("taking snapshot...")
            # Gather snapshot data
            guild = ctx.guild
            voice_channels = 0
            text_channels = 0
            categories = 0
            for channel in guild.channels:
                channel_type = str(channel.type).lower()
                if channel_type == "text":
                    text_channels += 1
                elif channel_type == "voice":
                    voice_channels += 1
                elif channel_type == "category":
                    categories += 1
            timestamp = datetime.now(timezone.utc)
            member_count = guild.member_count
            channel_count = len(guild.channels)
            role_count = len(guild.roles)
            bots = len([m for m in guild.members if m.bot])
            is_auto = False
            # Insert into SQLite
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT INTO snapshots (guild_id, guild_name, timestamp, member_count, channel_count, text_channels, voice_channels, categories, role_count, bots, is_auto)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(guild.id),
                guild.name,
                timestamp.isoformat(),
                member_count,
                channel_count,
                text_channels,
                voice_channels,
                categories,
                role_count,
                bots,
                int(is_auto)
            ))
            conn.commit()
            conn.close()
            try:
                await msg.edit(content=f""" **new snapshot**
                
**server**: {guild.name}
**members**: {member_count:,}
**channels**: {channel_count:,}
**time**: {format_time_in_timezone(timestamp, "%H:%M:%S")}""")
            except Exception as e:
                print(f"Error editing snapshot message: {str(e)}", type_="ERROR")
        
        elif cmd == "report":
            msg = None
            current_private = getConfigData().get("private")
            try:
                msg = await ctx.send("generating analytics report...")
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM snapshots WHERE guild_id = ?", (str(ctx.guild.id),))
                snap_count = c.fetchone()[0]

                if snap_count < 2:
                    error_msg = "Not enough data to generate a report. "
                    if not is_db_migrated():
                        error_msg += "Your data may still be in JSON format. Please run `<p>analytics migrate`."
                        script_log(f"Report generation failed for guild {ctx.guild.id}: DB not migrated.", level="ERROR")
                    else:
                        error_msg += f"Found only {snap_count} snapshot(s). A minimum of 2 is required. Please use `<p>analytics snapshot` to create more."
                        script_log(f"Report generation failed for guild {ctx.guild.id}: Insufficient snapshots ({snap_count}).", level="ERROR")
                    
                    if msg:
                        await msg.edit(content=error_msg)
                    else:
                        await ctx.send(error_msg)
                    conn.close()
                    return

                c.execute("SELECT timestamp, member_count, channel_count, text_channels, voice_channels, categories, role_count, bots, is_auto FROM snapshots WHERE guild_id = ? ORDER BY timestamp ASC", (str(ctx.guild.id),))
                rows = c.fetchall()
                conn.close()

                updateConfigData("private", False)
                snapshots = [
                    {
                        "timestamp": row[0], "member_count": row[1], "channel_count": row[2],
                        "text_channels": row[3], "voice_channels": row[4], "categories": row[5],
                        "role_count": row[6], "bots": row[7], "is_auto": bool(row[8])
                    }
                    for row in rows
                ]
                latest = snapshots[-1]
                oldest = snapshots[0]
                growth = latest["member_count"] - oldest["member_count"]
                growth_rate = (growth / oldest["member_count"]) * 100 if oldest["member_count"] > 0 else 0
                peak_members = max(s["member_count"] for s in snapshots)
                current_members = latest["member_count"]
                # Calculate difference from peak (current - peak)
                peak_diff = current_members - peak_members
                trend_data = analyze_growth_trend(snapshots)
                daily_change = trend_data["growth_rate_daily"]
                daily_growth_display = f"+{daily_change:.1f}" if daily_change >= 0 else f"{daily_change:.1f}"
                next_milestone = math.ceil(current_members / 1000) * 1000 if current_members >= 1000 else (1000 if current_members >= 500 else (500 if current_members >= 100 else 100))
                days_to_milestone = "∞"
                if daily_change > 0:
                    days_to_milestone = math.ceil((next_milestone - current_members) / daily_change)
                # --- demographics snippet ---
                demographics_snippet = ""
                # (Demographics will be refactored separately)
                booster_count = len(ctx.guild.premium_subscribers)
                # Format peak members with difference from current
                peak_diff_str = f" ({peak_diff:+,})" if peak_diff != 0 else " (0)"
                report = f"""## server overview\n\n**member statistics**\n• total members: **{current_members:,}**\n• peak members: **{peak_members:,}**{peak_diff_str}\n• bots: **{latest.get('bots', 0):,}**\n• human users: **{latest['member_count'] - latest.get('bots', 0):,}**\n• server boosters: **{booster_count}**\n\n**channel information**\n• total channels: **{latest['channel_count']:,}**\n• text channels: **{latest.get('text_channels', 0):,}**\n• voice channels: **{latest.get('voice_channels', 0):,}**\n• categories: **{latest.get('categories', 0):,}**\n\n**role count**\n• total roles: **{latest['role_count']:,}**\n\n**growth analysis**\n• current trend: **{trend_data['trend'].replace('_', ' ')}**\n• daily change: **{daily_growth_display}** members/day\n• member growth: **{growth:+,}** members total\n• growth rate: **{growth_rate:,.2f}%**\n• next milestone: **{next_milestone:,}** members\n• est. days to milestone: **{days_to_milestone}** days{demographics_snippet}\n\n*last updated: {format_time_in_timezone(datetime.fromisoformat(latest['timestamp']), '%y-%m-%d %h:%m')}*\nserver analytics\n"""
                await msg.delete()
                await forwardEmbedMethod(
                    channel_id=ctx.channel.id,
                    content=report,
                    title=f"server analytics report: {ctx.guild.name}",
                    image=None
                )
            except Exception as e:
                print(f"error generating report: {str(e)}", type_="error")
                if msg:
                    try:
                        await msg.edit(content=f"error generating analytics report for {ctx.guild.name}. please try again later.")
                    except Exception as e_edit:
                        print(f"Error editing report error message: {str(e_edit)}", type_="ERROR")
            finally:
                updateConfigData("private", current_private)
            
        elif cmd == "clear":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("DELETE FROM snapshots WHERE guild_id = ?", (str(ctx.guild.id),))
            conn.commit()
            conn.close()
            try:
                await ctx.send(f"analytics data for {ctx.guild.name} has been cleared.")
            except Exception as e:
                print(f"Error sending clear message: {str(e)}", type_="ERROR")
            
        elif cmd == "status":
            # Query snapshot count and config from DB
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM snapshots WHERE guild_id = ?", (str(ctx.guild.id),))
            snap_count = c.fetchone()[0]
            c.execute("SELECT auto_snapshot, last_auto_snapshot, first_snapshot_date, snapshot_retention_days FROM server_config WHERE guild_id = ?", (str(ctx.guild.id),))
            config_row = c.fetchone()
            conn.close()
            if config_row:
                auto_snapshot, last_auto_snapshot, first_snapshot_date, retention_days = config_row
            else:
                auto_snapshot, last_auto_snapshot, first_snapshot_date, retention_days = (0, 'never', 'unknown', DATA_RETENTION_DAYS)
            msg = f"""**analytics status for {ctx.guild.name}**\n\n• total snapshots: {snap_count}\n• data retention: {retention_days} days\n• auto snapshot: {'enabled' if auto_snapshot else 'disabled'}\n• last auto snapshot: {last_auto_snapshot or 'never'}\n• first snapshot: {first_snapshot_date or 'unknown'}\n"""
            await ctx.send(msg)
            
        elif cmd == "members":
            msg = await ctx.send("generating member graph...")
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT timestamp, member_count, channel_count, text_channels, voice_channels, categories, role_count, bots, is_auto FROM snapshots WHERE guild_id = ? ORDER BY timestamp DESC LIMIT 7", (str(ctx.guild.id),))
            rows = c.fetchall()
            conn.close()
            if not rows:
                await msg.edit(content="no analytics data available yet for this server.")
                return
            # Build recent_snapshots list from DB rows (newest first)
            recent_snapshots = [
                {
                    "timestamp": row[0],
                    "member_count": row[1],
                    "channel_count": row[2],
                    "text_channels": row[3],
                    "voice_channels": row[4],
                    "categories": row[5],
                    "role_count": row[6],
                    "bots": row[7],
                    "is_auto": bool(row[8])
                }
                for row in rows
            ]
            # Sort by timestamp descending (already is), group by member_count
            grouped_snapshots = []
            if recent_snapshots:
                current_group = [recent_snapshots[0]]
                for i in range(1, len(recent_snapshots)):
                    if recent_snapshots[i]["member_count"] == current_group[0]["member_count"]:
                        current_group.append(recent_snapshots[i])
                    else:
                        grouped_snapshots.append(current_group)
                        current_group = [recent_snapshots[i]]
                grouped_snapshots.append(current_group)
            graph_content = ""
            if len(recent_snapshots) >= 2:
                current_members = recent_snapshots[0]["member_count"]
                previous_members = recent_snapshots[1]["member_count"]
                overall_change = current_members - previous_members
                graph_content += f"**current members:** {current_members:,} ({overall_change:+,})\n\n"
            elif recent_snapshots:
                current_members = recent_snapshots[0]["member_count"]
                graph_content += f"**current members:** {current_members:,}\n\n"
            for i, group in enumerate(grouped_snapshots):
                latest_in_group = group[0]
                current_count = latest_in_group["member_count"]
                utc_time = datetime.fromisoformat(latest_in_group["timestamp"])
                change_str = ""
                if i < len(grouped_snapshots) - 1:
                    previous_group_count = grouped_snapshots[i + 1][0]["member_count"]
                    change = current_count - previous_group_count
                    if change > 0:
                        change_str = f" (+{change})"
                    elif change < 0:
                        change_str = f" ({change})"
                if len(group) > 1:
                    earliest_in_group = group[-1]
                    earliest_time = datetime.fromisoformat(earliest_in_group["timestamp"])
                    duration_hours = (utc_time - earliest_time).total_seconds() / 3600
                    date_str = format_time_in_timezone(utc_time, "%b %d")
                    time_range_str = f"{format_time_in_timezone(earliest_time, '%I:%M %p')} → {format_time_in_timezone(utc_time, '%I:%M %p')}"
                    graph_content += f"**{date_str}:** {time_range_str} ({duration_hours:.1f}h)\n"
                    graph_content += f"members: **{current_count:,}**{change_str}\n\n"
                else:
                    time_str = format_time_in_timezone(utc_time)
                    is_auto = latest_in_group.get("is_auto", False)
                    source = " (auto)" if is_auto else ""
                    graph_content += f"**{time_str}**{source}\n"
                    graph_content += f"members: **{current_count:,}**{change_str}\n\n"
            await msg.delete()
            await forwardEmbedMethod(
                channel_id=ctx.channel.id,
                content=graph_content,
                title=f"member count history - {ctx.guild.name}",
                image=None
            )
            
        elif cmd == "trend":
            msg = await ctx.send("analyzing growth trends...")
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT timestamp, member_count FROM snapshots WHERE guild_id = ? ORDER BY timestamp ASC", (str(ctx.guild.id),))
            rows = c.fetchall()
            conn.close()
            if len(rows) < 2:
                await msg.edit(content="not enough data for trend analysis. please take at least 2 snapshots.")
                return
            # Build snapshots list from DB rows
            snapshots = [
                {"timestamp": row[0], "member_count": row[1]} for row in rows
            ]
            # Get trend analysis for different time periods
            short_trend = analyze_growth_trend(snapshots, days=3)
            medium_trend = analyze_growth_trend(snapshots, days=7)
            long_trend = analyze_growth_trend(snapshots, days=14)
            latest = snapshots[-1]
            current_members = latest["member_count"]
            trend_content = f"""## member growth analysis\n\n**current members:** {current_members:,}\n\n**short-term trend:** {short_trend["trend"].replace('_', ' ')}\n• daily change: **{short_trend["growth_rate_daily"]:.1f}** members/day\n• 7-day projection: **{short_trend["prediction_7_days"]:,}** members\n• confidence: {short_trend["confidence"]}\n\n**medium-term trend:** {medium_trend["trend"].replace('_', ' ')}\n• daily change: **{medium_trend["growth_rate_daily"]:.1f}** members/day\n• 30-day projection: **{medium_trend["prediction_30_days"]:,}** members\n• confidence: {medium_trend["confidence"]}\n\n**long-term trend:** {long_trend["trend"].replace('_', ' ')}\n• over {long_trend["days_measured"]} days\n• total change: **{long_trend["growth_total"]:+,}** members\n\n*note: projections are estimates based on current trends*\n*last updated: {format_time_in_timezone(datetime.fromisoformat(latest['timestamp']), '%y-%m-%d %h:%m')}*\n"""
            await msg.delete()
            await forwardEmbedMethod(
                channel_id=ctx.channel.id,
                content=trend_content,
                title=f"member growth analysis - {ctx.guild.name}",
                image=None
            )
            
        elif cmd == "compare":
            days = 7
            if subcmd and subcmd.isdigit():
                days = int(subcmd)
            await compare_periods(ctx, days)
            
        elif cmd == "export":
            msg = await ctx.send("exporting data to file...")
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT timestamp, member_count, channel_count, text_channels, voice_channels, categories, role_count, bots FROM snapshots WHERE guild_id = ? ORDER BY timestamp ASC", (str(ctx.guild.id),))
            rows = c.fetchall()
            conn.close()
            if not rows:
                await msg.edit(content="no analytics data available to export.")
                return
            export_text = f"# server analytics export - {ctx.guild.name}\n"
            export_text += f"# generated on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} utc\n"
            export_text += f"# total snapshots: {len(rows)}\n\n"
            export_text += "timestamp,member_count,channel_count,text_channels,voice_channels,categories,role_count,bots\n"
            for row in rows:
                export_text += f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},{row[7]}\n"
            export_dir = Path(getScriptsPath()) / "exports"
            export_dir.mkdir(parents=True, exist_ok=True)
            filename = f"{ctx.guild.id}_analytics_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            export_path = export_dir / filename
            with open(export_path, "w", encoding='utf-8') as f:
                f.write(export_text)
            await msg.delete()
            await ctx.send(f"""analytics data export complete

file: `{filename}`
location: `{str(export_path)}`
snapshots: {len(rows)}
format: csv (comma-separated values)

*use your file manager to access the exported data*""")

        elif cmd == "auto":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            if subcmd in ["on", "true", "yes", "enable", "1"]:
                c.execute("INSERT OR REPLACE INTO server_config (guild_id, auto_snapshot) VALUES (?, ?)", (str(ctx.guild.id), 1))
                conn.commit()
                await ctx.send("automatic daily snapshots enabled")
            elif subcmd in ["off", "false", "no", "disable", "0"]:
                c.execute("INSERT OR REPLACE INTO server_config (guild_id, auto_snapshot) VALUES (?, ?)", (str(ctx.guild.id), 0))
                conn.commit()
                await ctx.send("automatic daily snapshots disabled")
            else:
                c.execute("SELECT auto_snapshot FROM server_config WHERE guild_id = ?", (str(ctx.guild.id),))
                row = c.fetchone()
                is_enabled = bool(row[0]) if row else False
                status = "enabled" if is_enabled else "disabled"
                await ctx.send(f""" **auto-snapshot status**
                
automatic daily snapshots are currently {status}.

use `<p>analytics auto on` to enable
use `<p>analytics auto off` to disable""")
            conn.close()

        elif cmd == "retention":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            if subcmd.isdigit():
                days = int(subcmd)
                c.execute("INSERT OR REPLACE INTO server_config (guild_id, snapshot_retention_days) VALUES (?, ?)", (str(ctx.guild.id), days))
                conn.commit()
                await ctx.send(f"data retention set to {days} days")
            else:
                c.execute("SELECT snapshot_retention_days FROM server_config WHERE guild_id = ?", (str(ctx.guild.id),))
                row = c.fetchone()
                current = row[0] if row and row[0] is not None else DATA_RETENTION_DAYS
                await ctx.send(f"data retention is {current} days")
            conn.close()

        elif cmd == "interval":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            if subcmd:
                try:
                    hours = float(subcmd)
                    c.execute("INSERT OR REPLACE INTO server_config (guild_id, auto_snapshot_interval_hours) VALUES (?, ?)", (str(ctx.guild.id), hours))
                    conn.commit()
                    await ctx.send(f"automatic snapshot interval set to {hours} hours")
                except ValueError:
                    await ctx.send("invalid subcommand. use `<p>analytics help` for a list of commands.")
            else:
                c.execute("SELECT auto_snapshot_interval_hours FROM server_config WHERE guild_id = ?", (str(ctx.guild.id),))
                row = c.fetchone()
                current = row[0] if row and row[0] is not None else DEFAULT_AUTO_SNAPSHOT_INTERVAL_HOURS
                await ctx.send(f"automatic snapshot interval is {current} hours")
            conn.close()
        
        # --- TIMEZONE SUBCOMMAND HANDLING ---
        elif cmd == "timezone":
            zone = subcmd.upper() if subcmd else None
            if not zone:
                await ctx.send(f"please specify a timezone. supported: {', '.join(TIMEZONE_OFFSETS.keys())}")
                return
            if zone not in TIMEZONE_OFFSETS:
                await ctx.send(f"invalid timezone. supported: {', '.join(TIMEZONE_OFFSETS.keys())}")
                return
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO server_config (guild_id, timezone) VALUES (?, ?)", (str(ctx.guild.id), zone))
            conn.commit()
            conn.close()
            await ctx.send(f"timezone set to `{zone}`. all times will now display in this timezone.")

        elif cmd == "demographics":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            if not subcmd:
                # Show summary for current server
                c.execute("SELECT name, account_created, joined_at FROM demographics WHERE guild_id = ?", (str(ctx.guild.id),))
                members = [dict(name=row[0], account_created=row[1], joined_at=row[2]) for row in c.fetchall()]
                print(f"[DEBUG] Read {len(members)} members from SQL for guild {ctx.guild.id}", type_="INFO")
                if not members:
                    msg = await ctx.send("No demographics data found. Fetching all members for initial demographics...")
                    try:
                        fetched = 0
                        # Try to find an accessible text channel
                        text_channel = None
                        for ch in ctx.guild.channels:
                            if str(ch.type).lower() == "text" and ch.permissions_for(ctx.guild.me).read_messages:
                                text_channel = ch
                                break
                        if not text_channel:
                            await msg.edit(content="No accessible text channel found. Please specify a channel ID with `<p>analytics demographics fetch <channel_id>`.")
                            conn.close()
                            return
                        members_list = await text_channel.guild.fetch_members()
                        print(f"[DEBUG] fetch_members() returned {len(members_list)} members", type_="INFO")
                        for member in members_list:
                            c.execute("INSERT OR REPLACE INTO demographics (guild_id, member_id, name, account_created, joined_at) VALUES (?, ?, ?, ?, ?)", (
                                str(ctx.guild.id), str(member.id), str(member), member.created_at.isoformat() if member.created_at else None, member.joined_at.isoformat() if member.joined_at else None
                            ))
                            fetched += 1
                        conn.commit()
                        print(f"[DEBUG] Inserted {fetched} members into SQL for guild {ctx.guild.id}", type_="INFO")
                        await msg.edit(content=f"Initial demographics data populated/updated for {fetched} members. Showing summary...")
                        c.execute("SELECT name, account_created, joined_at FROM demographics WHERE guild_id = ?", (str(ctx.guild.id),))
                        members = [dict(name=row[0], account_created=row[1], joined_at=row[2]) for row in c.fetchall()]
                        print(f"[DEBUG] After insert, {len(members)} members in SQL for guild {ctx.guild.id}", type_="INFO")
                    except Exception as e:
                        print(f"[DEBUG] Exception during demographics fetch/insert: {e}", type_="ERROR")
                        await msg.edit(content=f"Failed to fetch members: {e}\nIf this is a channel error, try `<p>analytics demographics fetch <channel_id>`. ")
                        conn.close()
                        return
                if members:
                    oldest_acc = min(members, key=lambda m: m["account_created"] or "9999")
                    newest_acc = max(members, key=lambda m: m["account_created"] or "0000")
                    longest_mem = min(members, key=lambda m: m["joined_at"] or "9999")
                    newest_mem = max(members, key=lambda m: m["joined_at"] or "0000")
                    msg = f"""## server demographics\n\n**account creation**\n• oldest tracked: {oldest_acc['name']} ({oldest_acc['account_created'][:10] if oldest_acc['account_created'] else 'unknown'})\n• newest tracked: {newest_acc['name']} ({newest_acc['account_created'][:10] if newest_acc['account_created'] else 'unknown'})\n\n**server join**\n• longest tracked: {longest_mem['name']} (joined {longest_mem['joined_at'][:10] if longest_mem['joined_at'] else 'unknown'})\n• newest tracked: {newest_mem['name']} (joined {newest_mem['joined_at'][:10] if newest_mem['joined_at'] else 'unknown'})\n\n*total tracked: {len(members)}*"""
                    await ctx.send(msg)
                else:
                    print(f"[DEBUG] No members found in SQL after fetch/insert for guild {ctx.guild.id}", type_="ERROR")
                conn.close()
            elif subcmd == "fetch":
                # Optionally allow a channel ID: <p>analytics demographics fetch <channel_id>
                channel_id = subarg.strip() if subarg else None
                msg = await ctx.send("Fetching all members for demographics...")
                try:
                    fetched = 0
                    text_channel = None
                    if channel_id:
                        text_channel = ctx.guild.get_channel(int(channel_id))
                        if not text_channel or str(text_channel.type).lower() != "text" or not text_channel.permissions_for(ctx.guild.me).read_messages:
                            await msg.edit(content="Invalid or inaccessible channel ID. Please specify a valid text channel ID.")
                            conn.close()
                            return
                    else:
                        for ch in ctx.guild.channels:
                            if str(ch.type).lower() == "text" and ch.permissions_for(ctx.guild.me).read_messages:
                                text_channel = ch
                                break
                    if not text_channel:
                        await msg.edit(content="No accessible text channel found. Please specify a channel ID with `<p>analytics demographics fetch <channel_id>`.")
                        conn.close()
                        return
                    members_list = await text_channel.guild.fetch_members()
                    for member in members_list:
                        c.execute("INSERT OR REPLACE INTO demographics (guild_id, member_id, name, account_created, joined_at) VALUES (?, ?, ?, ?, ?)", (
                            str(ctx.guild.id), str(member.id), str(member), member.created_at.isoformat() if member.created_at else None, member.joined_at.isoformat() if member.joined_at else None
                        ))
                        fetched += 1
                    conn.commit()
                    c.execute("SELECT COUNT(*) FROM demographics WHERE guild_id = ?", (str(ctx.guild.id),))
                    total = c.fetchone()[0]
                    await msg.edit(content=f"Fetched/updated demographics for {fetched} members. Total tracked: {total}.")
                except Exception as e:
                    await msg.edit(content=f"Failed to fetch members: {e}\nIf this is a channel error, try `<p>analytics demographics fetch <channel_id>`. ")
                conn.close()
            elif subcmd == "list":
                c.execute("SELECT guild_id FROM demographics_servers")
                servers = [row[0] for row in c.fetchall()]
                if not servers:
                    await ctx.send("no servers are currently tracked for demographics.")
                else:
                    await ctx.send("tracked servers:\n" + "\n".join(servers))
                conn.close()
            elif subcmd == "remove" and subarg:
                server_id = subarg.strip()
                c.execute("DELETE FROM demographics_servers WHERE guild_id = ?", (server_id,))
                conn.commit()
                await ctx.send(f"server `{server_id}` removed from demographics tracking.")
                conn.close()
            else:
                await ctx.send("usage: <p>analytics demographics [fetch|list|remove <server_id>]")

        elif cmd == "migrate":
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            msg = await ctx.send("Starting migration to SQLite DB...")
            try:
                create_schema()
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                
                # Migration statistics
                migration_stats = defaultdict(lambda: {
                    'total': 0, 'success': 0, 'skipped': 0, 'errors': []
                })

                # For each server directory
                if not os.path.isdir(TEST_DATA_DIR):
                    await msg.edit(content=f"Test data directory not found: {TEST_DATA_DIR}")
                    return

                for server_id in os.listdir(TEST_DATA_DIR):
                    server_dir = os.path.join(TEST_DATA_DIR, server_id)
                    if not os.path.isdir(server_dir):
                        continue

                    script_log(f"Processing server: {server_id}", level="INFO")

                    # --- Snapshots Migration ---
                    snap_path = os.path.join(server_dir, "member_snapshots.json")
                    if os.path.isfile(snap_path):
                        try:
                            with open(snap_path, "r", encoding="utf-8") as f:
                                data = json.load(f).get("snapshots", [])
                                migration_stats[server_id]['total'] += len(data)
                                for i, snap in enumerate(data):
                                    try:
                                        c.execute("""
                                            INSERT INTO snapshots (guild_id, guild_name, timestamp, member_count, channel_count, text_channels, voice_channels, categories, role_count, bots, is_auto)
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """, (
                                            server_id, snap.get("name"), snap.get("timestamp"), snap.get("member_count"),
                                            snap.get("channel_count"), snap.get("text_channels"), snap.get("voice_channels"),
                                            snap.get("categories"), snap.get("role_count"), snap.get("bots"),
                                            int(snap.get("is_auto", False))
                                        ))
                                        migration_stats[server_id]['success'] += 1
                                    except Exception as e:
                                        migration_stats[server_id]['skipped'] += 1
                                        err_msg = f"Skipping malformed snapshot record #{i+1} for server {server_id}: {e}"
                                        migration_stats[server_id]['errors'].append(err_msg)
                                        script_log(err_msg, level="ERROR", exc_info=True)
                        except json.JSONDecodeError as e:
                            err_msg = f"Could not parse snapshots JSON for server {server_id}: {e}"
                            migration_stats[server_id]['errors'].append(err_msg)
                            script_log(err_msg, level="ERROR", exc_info=True)

                    # --- Demographics Migration ---
                    demo_path = os.path.join(server_dir, "member_demographics.json")
                    if os.path.isfile(demo_path):
                        try:
                            with open(demo_path, "r", encoding="utf-8") as f:
                                demo_data = json.load(f)
                                for member_id, info in demo_data.items():
                                    try:
                                        c.execute("""
                                            INSERT OR REPLACE INTO demographics (guild_id, member_id, name, account_created, joined_at)
                                            VALUES (?, ?, ?, ?, ?)
                                        """, (server_id, member_id, info.get("name"), info.get("account_created"), info.get("joined_at")))
                                    except Exception as e:
                                        script_log(f"Skipping malformed demographics record for member {member_id} in server {server_id}: {e}", level="ERROR", exc_info=True)
                        except json.JSONDecodeError as e:
                            script_log(f"Could not parse demographics JSON for server {server_id}: {e}", level="ERROR", exc_info=True)

                    # --- Config Migration ---
                    config_path = os.path.join(server_dir, "analytics_config.json")
                    if os.path.isfile(config_path):
                        try:
                            with open(config_path, "r", encoding="utf-8") as f:
                                config = json.load(f)
                            c.execute("""
                                INSERT OR REPLACE INTO server_config (guild_id, auto_snapshot, last_auto_snapshot, first_snapshot_date, chart_style, snapshot_retention_days, auto_snapshot_interval_hours)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (
                                server_id, int(config.get("auto_snapshot", 0)), config.get("last_auto_snapshot"),
                                config.get("first_snapshot_date"), config.get("chart_style"), config.get("snapshot_retention_days"),
                                config.get("auto_snapshot_interval_hours")
                            ))
                        except Exception as e:
                            script_log(f"Could not migrate config for server {server_id}: {e}", level="ERROR", exc_info=True)

                # --- Demographics Servers List Migration ---
                if os.path.isfile(DEMO_SERVERS_FILE):
                    try:
                        with open(DEMO_SERVERS_FILE, "r", encoding="utf-8") as f:
                            servers = json.load(f)
                        for sid in servers:
                            c.execute("INSERT OR IGNORE INTO demographics_servers (guild_id) VALUES (?)", (sid,))
                    except Exception as e:
                        script_log(f"Error migrating demographics_servers.json: {e}", level="ERROR", exc_info=True)

                conn.commit()
                conn.close()
                set_db_migrated()

                # --- Build Final Summary Report ---
                summary = "**Migration to SQLite Complete!**\n\n"
                total_snapshots_migrated = sum(s['success'] for s in migration_stats.values())
                total_snapshots_skipped = sum(s['skipped'] for s in migration_stats.values())
                summary += f"**Snapshots:** Migrated **{total_snapshots_migrated}** records, skipped **{total_snapshots_skipped}**.\n"
                
                if total_snapshots_skipped > 0:
                    summary += "_Skipped records are logged to the console if debug mode is on._\n"

                summary += "\n**Migration Details by Server:**\n"
                for server_id, stats in migration_stats.items():
                    if stats['skipped'] > 0:
                        summary += f"- **{server_id}:** Migrated {stats['success']}/{stats['total']} snapshots. **({stats['skipped']} skipped)**\n"
                    else:
                        summary += f"- **{server_id}:** Migrated {stats['success']}/{stats['total']} snapshots.\n"

                await msg.edit(content=summary)
                script_log("Database migration completed successfully.", level="SUCCESS")

            except Exception as e:
                await msg.edit(content=f"A critical error occurred during migration. Check logs for details.")
                script_log("Migration failed critically.", level="ERROR", exc_info=True)

        elif cmd == "dbstats":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            # Per-guild stats
            c.execute("SELECT COUNT(*) FROM snapshots WHERE guild_id = ?", (str(ctx.guild.id),))
            snap_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM demographics WHERE guild_id = ?", (str(ctx.guild.id),))
            demo_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM server_config WHERE guild_id = ?", (str(ctx.guild.id),))
            config_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM demographics_servers WHERE guild_id = ?", (str(ctx.guild.id),))
            demo_servers_count = c.fetchone()[0]
            # Global stats
            c.execute("SELECT COUNT(*) FROM snapshots")
            snap_count_all = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM demographics")
            demo_count_all = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM server_config")
            config_count_all = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM demographics_servers")
            demo_servers_count_all = c.fetchone()[0]
            conn.close()
            msg = f"""**Database Statistics**

__Current Guild__
• Snapshots: {snap_count}
• Demographics: {demo_count}
• Server Config: {config_count}
• Demographics Servers: {demo_servers_count}

__Global__
• Snapshots: {snap_count_all}
• Demographics: {demo_count_all}
• Server Config: {config_count_all}
• Demographics Servers: {demo_servers_count_all}
"""
            await ctx.send(msg)

        elif cmd == "debug":
            # Toggle debug logging for this script
            SCRIPT_NAME = "Server Analytics - DB Edition"
            debug_key = f"{SCRIPT_NAME}_debug_enabled"
            current = getConfigData().get(debug_key, False)
            new_state = not current
            updateConfigData(debug_key, new_state)
            await ctx.send(f"{SCRIPT_NAME} debug logging {'enabled' if new_state else 'disabled'}.")
            return

        elif cmd == "holylogger":
            msg = await ctx.send("Starting holylogger - scanning all servers for unmonitored ones...")
            try:
                # Ensure database schema exists
                create_schema()
                
                # Get all servers the bot is in
                all_guilds = list(bot.guilds)
                print(f"[HOLYLOGGER] Found {len(all_guilds)} total servers", type_="INFO")
                
                # Get currently monitored servers from database
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT DISTINCT guild_id FROM snapshots")
                monitored_guilds = {row[0] for row in c.fetchall()}
                conn.close()
                
                print(f"[HOLYLOGGER] Currently monitoring {len(monitored_guilds)} servers", type_="INFO")
                
                # Find unmonitored servers
                unmonitored_guilds = []
                for guild in all_guilds:
                    if str(guild.id) not in monitored_guilds:
                        unmonitored_guilds.append(guild)
                
                print(f"[HOLYLOGGER] Found {len(unmonitored_guilds)} unmonitored servers", type_="INFO")
                
                if not unmonitored_guilds:
                    await msg.edit(content="All servers are already being monitored! No action needed.")
                    return
                
                # Process each unmonitored server
                processed_count = 0
                failed_count = 0
                total_members_fetched = 0
                
                await msg.edit(content=f"Found {len(unmonitored_guilds)} unmonitored servers. Starting processing with 10-second intervals...")
                
                for i, guild in enumerate(unmonitored_guilds, 1):
                    try:
                        print(f"[HOLYLOGGER] Processing server {i}/{len(unmonitored_guilds)}: {guild.name} (ID: {guild.id})", type_="INFO")
                        
                        # Take snapshot
                        print(f"[HOLYLOGGER] Taking snapshot for {guild.name}", type_="INFO")
                        await take_snapshot(guild, is_auto=False)
                        
                        # Wait 5 seconds before fetching members
                        await asyncio.sleep(5)
                        
                        # Fetch members
                        print(f"[HOLYLOGGER] Fetching members for {guild.name}", type_="INFO")
                        try:
                            members_list = await guild.fetch_members()
                            print(f"[HOLYLOGGER] Fetched {len(members_list)} members from {guild.name}", type_="INFO")
                            
                            # Insert members into demographics table
                            conn = sqlite3.connect(DB_PATH)
                            c = conn.cursor()
                            fetched_count = 0
                            for member in members_list:
                                c.execute("INSERT OR REPLACE INTO demographics (guild_id, member_id, name, account_created, joined_at) VALUES (?, ?, ?, ?, ?)", (
                                    str(guild.id), 
                                    str(member.id), 
                                    str(member), 
                                    member.created_at.isoformat() if member.created_at else None, 
                                    member.joined_at.isoformat() if member.joined_at else None
                                ))
                                fetched_count += 1
                            conn.commit()
                            conn.close()
                            
                            total_members_fetched += fetched_count
                            print(f"[HOLYLOGGER] Inserted {fetched_count} members into database for {guild.name}", type_="INFO")
                            
                        except Exception as member_error:
                            print(f"[HOLYLOGGER] Failed to fetch members for {guild.name}: {member_error}", type_="ERROR")
                        
                        processed_count += 1
                        print(f"[HOLYLOGGER] Successfully processed {guild.name} ({processed_count}/{len(unmonitored_guilds)})", type_="INFO")
                        
                        # Wait 10 seconds before next server (except for the last one)
                        if i < len(unmonitored_guilds):
                            await asyncio.sleep(10)
                            
                    except Exception as guild_error:
                        failed_count += 1
                        print(f"[HOLYLOGGER] Failed to process {guild.name}: {guild_error}", type_="ERROR")
                        continue
                
                # Final summary
                summary = f"""**HOLYLOGGER COMPLETE**

**Results:**
• Total servers scanned: {len(all_guilds)}
• Unmonitored servers found: {len(unmonitored_guilds)}
• Successfully processed: {processed_count}
• Failed to process: {failed_count}
• Total members fetched: {total_members_fetched:,}

**Status:** {'All servers now monitored!' if failed_count == 0 else f'{failed_count} servers failed to process'}

*Check console logs for detailed information*"""
                
                await msg.edit(content=summary)
                print(f"[HOLYLOGGER] Completed! Processed {processed_count}/{len(unmonitored_guilds)} servers, fetched {total_members_fetched:,} total members", type_="SUCCESS")
                
            except Exception as e:
                error_msg = f"Error in holylogger: {str(e)}"
                print(f"[HOLYLOGGER] {error_msg}", type_="ERROR")
                await msg.edit(content=error_msg)

        elif cmd == "api":
            if subcmd == "start":
                try:
                    if hasattr(bot, 'api_runner'):
                        await ctx.send("API server is already running!")
                        return
                    
                    await ctx.send("Starting API server...")
                    api_runner = await start_api_server()
                    bot.api_runner = api_runner
                    await ctx.send(f"✅ API server started successfully on {API_HOST}:{API_PORT}")
                except Exception as e:
                    await ctx.send(f"❌ Failed to start API server: {e}")
            
            elif subcmd == "status":
                if hasattr(bot, 'api_runner'):
                    await ctx.send(f"✅ API server is running on {API_HOST}:{API_PORT}")
                else:
                    await ctx.send("❌ API server is not running")
            
            elif subcmd == "stop":
                if hasattr(bot, 'api_runner'):
                    try:
                        await bot.api_runner.cleanup()
                        delattr(bot, 'api_runner')
                        await ctx.send("✅ API server stopped")
                    except Exception as e:
                        await ctx.send(f"❌ Failed to stop API server: {e}")
                else:
                    await ctx.send("❌ API server is not running")
            
            else:
                await ctx.send("""**API Commands:**
• `analytics api start` - Start the API server manually
• `analytics api status` - Check if API server is running
• `analytics api stop` - Stop the API server""")

        elif cmd == "boosters":
            boosters = ctx.guild.premium_subscribers
            if not boosters:
                await ctx.send("This server has no boosters.")
                return
            booster_list = []
            for booster in boosters:
                booster_list.append(f"{booster.name} ({booster.id})")
            await forwardEmbedMethod(
                channel_id=ctx.channel.id,
                title=f"Server Boosters - {ctx.guild.name}",
                content="\n".join(booster_list)
            )
        elif cmd == "resetdb" or (cmd == "reset" and subcmd == "database"):
            if subcmd != "confirm" and subarg.lower() != "confirm":
                await ctx.send("""⚠️ **DANGER ZONE: Database Reset** ⚠️\n\nThis will **DELETE ALL ANALYTICS DATA** (snapshots, demographics, configs) and cannot be undone.\n\nTo proceed, type:\n`<p>analytics resetdb confirm`\n\n**Are you sure?**""")
                return
            msg = await ctx.send("Wiping analytics database and all related files...")
            try:
                # Remove the SQLite DB file
                db_path = os.path.join(getScriptsPath(), "json", "analytics_test.db")
                if os.path.isfile(db_path):
                    os.remove(db_path)
                # Remove server_member_tracking directory
                tracking_dir = os.path.join(getScriptsPath(), "json", "server_member_tracking")
                if os.path.isdir(tracking_dir):
                    shutil.rmtree(tracking_dir)
                # Remove demographics_servers.json
                demo_servers_file = os.path.join(getScriptsPath(), "json", "demographics_servers.json")
                if os.path.isfile(demo_servers_file):
                    os.remove(demo_servers_file)
                # Recreate empty DB
                create_schema()
                await msg.edit(content="✅ Analytics database and all related files have been wiped. The system is now reset. You may need to refresh the dashboard UI.")
            except Exception as e:
                await msg.edit(content=f"❌ Failed to reset database: {e}")

        else:
            await ctx.send(f'command "{user_input}" not found, use analytics help for a list of commands.')
            
    # Compare server stats between time periods
    async def compare_periods(ctx, days=7):
        await ctx.send(f"comparing current period with {days} days ago...")
        
        # Fetch snapshots from the database
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT timestamp, member_count, channel_count, text_channels, voice_channels, categories, role_count, bots FROM snapshots WHERE guild_id = ? ORDER BY timestamp ASC", (str(ctx.guild.id),))
        rows = c.fetchall()
        conn.close()

        snapshots = [
            {
                "timestamp": row[0],
                "member_count": row[1],
                "channel_count": row[2],
                "text_channels": row[3],
                "voice_channels": row[4],
                "categories": row[5],
                "role_count": row[6],
                "bots": row[7]
            }
            for row in rows
        ]
        
        # Save current private setting and update it
        current_private = getConfigData().get("private")
        updateConfigData("private", False)
        
        try:
            if len(snapshots) < 2:
                await forwardEmbedMethod(
                    channel_id=ctx.channel.id,
                    content="not enough data for comparison. please take at least 2 snapshots.",
                    title=f"comparison report: last {days} days - {ctx.guild.name}",
                    image=None
                )
                return
                
            # Get the most recent snapshot
            current = snapshots[-1]
            current_time = datetime.fromisoformat(current["timestamp"])
            
            # Find a snapshot from approximately 'days' days ago
            target_time = current_time - timedelta(days=days)
            previous = min(snapshots[:-1], 
                           key=lambda x: abs(datetime.fromisoformat(x["timestamp"]) - target_time))
            previous_time = datetime.fromisoformat(previous["timestamp"])
            
            # Calculate actual days between snapshots
            days_diff = (current_time - previous_time).total_seconds() / (24 * 3600)
            
            # Calculate differences
            member_diff = current["member_count"] - previous["member_count"]
            member_percent = (member_diff / previous["member_count"]) * 100 if previous["member_count"] > 0 else 0
            
            channel_diff = current["channel_count"] - previous["channel_count"]
            role_diff = current["role_count"] - previous["role_count"]
            
            # Format dates
            current_date = format_time_in_timezone(current_time, "%y-%m-%d %h:%m")
            previous_date = format_time_in_timezone(previous_time, "%y-%m-%d %h:%m")
            
            # Build comparison message
            comparison = f"""## server comparison

**time period:** {days_diff:.1f} days
**from:** {previous_date}
**to:** {current_date}

**member changes**
• before: **{previous["member_count"]:,}** members
• now: **{current["member_count"]:,}** members
• change: **{member_diff:+,}** members ({member_percent:+.2f}%)
• bots: **{current.get("bots", 0) - previous.get("bots", 0):+,}**

**channel changes**
• total: **{channel_diff:+,}** channels
• text: **{current.get("text_channels", 0) - previous.get("text_channels", 0):+,}**
• voice: **{current.get("voice_channels", 0) - previous.get("voice_channels", 0):+,}**

**role changes**
• total: **{role_diff:+,}** roles

*server comparison between two points in time*"""
            
            await forwardEmbedMethod(
                channel_id=ctx.channel.id,
                content=comparison,
                title=f"comparison report: last {days} days - {ctx.guild.name}",
                image=None
            )
        except Exception as e:
            print(f"error generating comparison: {str(e)}", type_="error")
            await ctx.send(f"error generating server comparison for {ctx.guild.name}. please try again later.")
        finally:
            # Always restore private setting
            updateConfigData("private", current_private)
            
    # Export analytics data to text file
    async def export_data(ctx):
        msg = None
        try:
            msg = await ctx.send("exporting data to file...")
            files = get_server_files(ctx.guild.id)
            data = load_data(files['snapshots'])
            snapshots = data.get("snapshots", [])
            
            if not snapshots:
                await msg.edit(content="no analytics data available to export.")
                return
                
            # Sort snapshots chronologically
            sorted_snapshots = sorted(snapshots, key=lambda x: x["timestamp"])
            
            # Create a formatted text file
            export_text = f"# server analytics export - {ctx.guild.name}\n"
            export_text += f"# generated on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} utc\n"
            export_text += f"# total snapshots: {len(snapshots)}\n\n"
            
            export_text += "timestamp,member_count,channel_count,text_channels,voice_channels,categories,role_count,bots\n"
            
            for snapshot in sorted_snapshots:
                timestamp = snapshot["timestamp"]
                member_count = snapshot["member_count"]
                channel_count = snapshot["channel_count"]
                text_channels = snapshot.get("text_channels", 0)
                voice_channels = snapshot.get("voice_channels", 0)
                categories = snapshot.get("categories", 0)
                role_count = snapshot["role_count"]
                bots = snapshot.get("bots", 0)
                export_text += f"{timestamp},{member_count},{channel_count},{text_channels},{voice_channels},{categories},{role_count},{bots}\n"
                
            # Create a temporary file in the exports directory
            export_dir = Path(getScriptsPath()) / "exports"
            export_dir.mkdir(parents=True, exist_ok=True)
            
            filename = f"{ctx.guild.id}_analytics_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            export_path = export_dir / filename
            
            with open(export_path, "w", encoding='utf-8') as f:
                f.write(export_text)
                
            await msg.delete()
            
            await ctx.send(f"""analytics data export complete

file: `{filename}`
location: `{str(export_path)}`
snapshots: {len(snapshots)}
format: csv (comma-separated values)

*use your file manager to access the exported data*""")
            
        except Exception as e:
            print(f"error exporting data: {str(e)}", type_="ERROR")
            if msg:
                try:
                    await msg.edit(content=f"error exporting data for {ctx.guild.name}. please try again later.")
                except Exception as e_edit:
                    print(f"Error editing export error message: {str(e_edit)}", type_="ERROR")
            await ctx.send(f"error exporting analytics data. please try again later.")

    # --- SQLite DB Setup ---
    DB_PATH = os.path.join(getScriptsPath(), "json", "analytics_test.db")
    TEST_DATA_DIR = os.path.join(getScriptsPath(), "json", "server_member_tracking")
    DEMO_SERVERS_FILE = os.path.join(getScriptsPath(), "json", "demographics_servers.json")

    def create_schema():
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id TEXT,
            guild_name TEXT,
            timestamp TEXT,
            member_count INTEGER,
            channel_count INTEGER,
            text_channels INTEGER,
            voice_channels INTEGER,
            categories INTEGER,
            role_count INTEGER,
            bots INTEGER,
            is_auto INTEGER
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS demographics (
            guild_id TEXT,
            member_id TEXT,
            name TEXT,
            account_created TEXT,
            joined_at TEXT,
            PRIMARY KEY (guild_id, member_id)
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS server_config (
            guild_id TEXT PRIMARY KEY,
            auto_snapshot INTEGER,
            last_auto_snapshot TEXT,
            first_snapshot_date TEXT,
            chart_style TEXT,
            snapshot_retention_days INTEGER,
            auto_snapshot_interval_hours REAL
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS demographics_servers (
            guild_id TEXT PRIMARY KEY
        )''')
        conn.commit()
        conn.close()

    def set_db_migrated():
        updateConfigData("analytics_db_migrated", True)

    def is_db_migrated():
        return getConfigData().get("analytics_db_migrated", False)

    def is_debug_enabled():
        """Checks if debug logging is enabled in config."""
        SCRIPT_NAME = "Server Analytics - DB Edition"
        debug_key = f"{SCRIPT_NAME}_debug_enabled"
        try:
            return getConfigData().get(debug_key, False)
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [Server Analytics - DB Edition] [ERROR] Error checking debug status: {e}", type_="ERROR")
            return False

    def script_log(message, level="INFO", exc_info=False):
        """
        Logs a message with timestamp, script name, and level.
        Respects the debug flag for INFO and SUCCESS messages.
        Optionally includes exception traceback.
        """
        level = level.upper()
        
        # Only log INFO/SUCCESS if debug is enabled
        if level in ["INFO", "SUCCESS"] and not is_debug_enabled():
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [Server Analytics - DB Edition] [{level}] {message}"

        if exc_info:
            exc_text = traceback.format_exc()
            if exc_text and exc_text != 'NoneType: None\n':
                 log_entry += f"\nTraceback:\n{exc_text}"

        print(log_entry, type_=level)

    # aiohttp micro-API setup
    async def fetch_members_handler(request):
        """Handle POST /fetch_members requests"""
        try:
            # Verify token
            auth_header = request.headers.get('Authorization')
            if not auth_header or auth_header != f'Bearer {API_TOKEN}':
                print(f"[WebAPI] Unauthorized request: {auth_header}", type_="ERROR")
                return web.json_response({'error': 'Unauthorized'}, status=401)
            
            # Parse request body
            data = await request.json()
            guild_id = data.get('guild_id')
            token = data.get('token')
            channel_id = data.get('channel_id')  # Optional
            print(f"[WebAPI] fetch_members_handler called with guild_id={guild_id}, channel_id={channel_id}", type_="INFO")
            
            if not guild_id or not token or token != API_TOKEN:
                print(f"[WebAPI] Invalid request data: guild_id={guild_id}, token={token}", type_="ERROR")
                return web.json_response({'error': 'Invalid request data'}, status=400)
            
            # Find the guild
            guild = bot.get_guild(int(guild_id))
            if not guild:
                print(f"[WebAPI] Guild not found or not accessible: {guild_id}", type_="ERROR")
                return web.json_response({'error': 'Guild not found or not accessible'}, status=404)
            
            # Try to fetch members using the specified channel or all accessible text channels
            try:
                text_channel = None
                if channel_id:
                    print(f"[WebAPI] Attempting to use provided channel_id={channel_id}", type_="INFO")
                    text_channel = guild.get_channel(int(channel_id))
                    if not text_channel or str(text_channel.type).lower() != "text" or not text_channel.permissions_for(guild.me).read_messages:
                        print(f"[WebAPI] Invalid or inaccessible channel_id={channel_id}", type_="ERROR")
                        return web.json_response({'error': 'Invalid or inaccessible channel ID.'}, status=400)
                    try:
                        members_list = await text_channel.guild.fetch_members()
                        print(f"[WebAPI] Successfully fetched members using channel_id={channel_id}", type_="INFO")
                    except Exception as e:
                        print(f"[WebAPI] Exception when fetching members with channel_id={channel_id}: {e}", type_="ERROR")
                        return web.json_response({'error': f'Failed to fetch members with channel_id={channel_id}: {e}'}, status=500)
                else:
                    # Try all accessible text channels
                    members_list = None
                    for ch in guild.channels:
                        if str(ch.type).lower() == "text" and ch.permissions_for(guild.me).read_messages:
                            print(f"[WebAPI] Trying channel {ch.id} ({ch.name})...", type_="INFO")
                            try:
                                members_list = await ch.guild.fetch_members()
                                text_channel = ch
                                print(f"[WebAPI] Successfully fetched members using channel {ch.id} ({ch.name})", type_="INFO")
                                break
                            except Exception as e:
                                print(f"[WebAPI] Exception in channel {ch.id} ({ch.name}): {e}", type_="ERROR")
                                continue
                    if members_list is None:
                        print(f"[WebAPI] Failed to automatically choose channels; please specify them manually", type_="ERROR")
                        return web.json_response({'error': 'Failed to automatically choose channels; please specify them manually'}, status=500)
                print(f"[WebAPI] Fetched {len(members_list)} members from {guild.name} using channel {text_channel.id if text_channel else 'N/A'}", type_="INFO")
                
                # Insert members into demographics table
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                fetched_count = 0
                
                for member in members_list:
                    c.execute("""
                        INSERT OR REPLACE INTO demographics (guild_id, member_id, name, account_created, joined_at) 
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        str(guild.id), 
                        str(member.id), 
                        str(member), 
                        member.created_at.isoformat() if member.created_at else None, 
                        member.joined_at.isoformat() if member.joined_at else None
                    ))
                    fetched_count += 1
                
                conn.commit()
                conn.close()
                
                print(f"[WebAPI] Successfully inserted {fetched_count} members into database for {guild.name}", type_="INFO")
                
                return web.json_response({
                    'success': True,
                    'guild_name': guild.name,
                    'members_fetched': fetched_count,
                    'message': f'Successfully fetched {fetched_count} members from {guild.name}',
                    'channel_id': text_channel.id if text_channel else None
                })
                
            except Exception as member_error:
                print(f"[WebAPI] Failed to fetch members for {guild.name}: {member_error}", type_="ERROR")
                return web.json_response({
                    'error': f'Failed to fetch members: {str(member_error)}'
                }, status=500)
                
        except Exception as e:
            print(f"[WebAPI] Error in fetch_members_handler: {e}", type_="ERROR")
            return web.json_response({'error': 'Internal server error'}, status=500)

    async def health_check_handler(request):
        """Handle GET /health requests for testing"""
        return web.json_response({
            'status': 'ok',
            'message': 'NightyScript micro-API is running',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })

    async def start_api_server():
        """Start the aiohttp micro-API server"""
        try:
            print(f"[WebAPI] Starting micro-API server on {API_HOST}:{API_PORT}...", type_="INFO")
            
            app = web.Application()
            app.router.add_post('/fetch_members', fetch_members_handler)
            app.router.add_get('/health', health_check_handler)
            
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, API_HOST, API_PORT)
            await site.start()
            
            print(f"[WebAPI] Micro-API server started successfully on {API_HOST}:{API_PORT}", type_="INFO")
            print(f"[WebAPI] Available endpoints:", type_="INFO")
            print(f"[WebAPI]   GET  /health - Health check", type_="INFO")
            print(f"[WebAPI]   POST /fetch_members - Fetch members", type_="INFO")
            return runner
            
        except Exception as e:
            print(f"[WebAPI] Failed to start API server: {e}", type_="ERROR")
            raise

    # Start the API server when the bot is ready
    @bot.listen("on_ready")
    async def setup_api_server():
        """Set up the aiohttp micro-API server"""
        print(f"[WebAPI] Bot ready event triggered, setting up API server...", type_="INFO")
        try:
            api_runner = await start_api_server()
            # Store the runner for cleanup if needed
            bot.api_runner = api_runner
            print(f"[WebAPI] API server setup completed successfully", type_="INFO")
        except Exception as e:
            print(f"[WebAPI] Failed to start API server: {e}", type_="ERROR")
            print(f"[WebAPI] Error details: {type(e).__name__}: {str(e)}", type_="ERROR")

server_analytics()  # Initialize the script
