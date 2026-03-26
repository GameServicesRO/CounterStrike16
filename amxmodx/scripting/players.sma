#include <amxmodx>
#include <geoip>
#include <sqlx>

#define QUERY_SIZE 1024
#define DEBOUNCE_PANEL_KEY_DB_REQUEST_TIME 2

#pragma semicolon 1

new Handle:g_hTuple;

new g_iJoinedTime[MAX_PLAYERS + 1];
new g_iDebouncePanelKeyDBRequestTime[MAX_PLAYERS + 1];

public plugin_init()
{
    register_plugin("[GS] Players", "0.6", "lexzor");

    register_concmd("players_generate_unique_keys", "players_generate_unique_keys_cmd");
    register_clcmd("amx_panel_key", "amx_panel_key_cmd");
}

public amx_panel_key_cmd(id)
{
    if(!is_user_connected(id))
        return PLUGIN_HANDLED;

    new currTime = get_systime();

    if(g_iDebouncePanelKeyDBRequestTime[id] > currTime)
    {
        new waitSeconds = g_iDebouncePanelKeyDBRequestTime[id] - currTime;
        client_print(id, print_console, "[GS] You have to wait %i second%s before retrieving key from database again.", waitSeconds, waitSeconds > 1 ? "s" : "");
        return PLUGIN_HANDLED;
    }

    new authid[MAX_AUTHID_LENGTH];
    get_user_authid(id, authid, charsmax(authid));

    new data[1];
    data[0] = id;

    SQL_ThreadQuery(g_hTuple, "OnPlayerKeyRetrieved", fmt("SELECT `unique_key` FROM `players` WHERE `steamid` = '%s'", authid), data, sizeof(data));

    client_print(id, print_console, "[GS] Loading key from database...");

    g_iDebouncePanelKeyDBRequestTime[id] = currTime + DEBOUNCE_PANEL_KEY_DB_REQUEST_TIME;

    return PLUGIN_HANDLED;
}

public OnPlayerKeyRetrieved(failstate, Handle:query, error[], errnum, data[], size, Float:queuetime)
{
    if(failstate || errnum)
    {
        log_amx("[LINE: %i] An SQL Error has been encoutered. Error code %i^nError: %s", __LINE__, errnum, error);
        SQL_FreeHandle(query);
        return;
    }

    new const id = data[0];
    new const bool:isConnected = bool:is_user_connected(id);

    if(SQL_NumResults(query) > 1)
    {
        SQL_FreeHandle(query);

        log_amx("Too many keys retrieved from database for %n (duplicate SteamIDs in database)", isConnected ? id : -1);
        if(isConnected)
        {
            client_print(id, print_console, "[GS] Too many keys retrieved from database because of duplicate SteamIDs. Please contact owner of the server!");
            return;
        }
    }

    if(!SQL_NumResults(query))
    {
        SQL_FreeHandle(query);
        
        log_amx("No key retrieved for %n (no SteamID in database)", isConnected ? id : -1);
        if(isConnected)
        {
            client_print(id, print_console, "[GS] Key not found because your SteamID does not exists in database. Please contact owner of the server!");
            return;
        }
    }

    new uniqueKey[33];
    SQL_ReadResult(query, SQL_FieldNameToNum(query, "unique_key"), uniqueKey, charsmax(uniqueKey));

    if(isConnected)
    {
        client_print(id, print_console, "[GS] Key retrieved from database: %s", uniqueKey);
    }

    SQL_FreeHandle(query);
}

public plugin_cfg()
{
    new errorCode;
    new errorStr[700];

    g_hTuple = SQL_MakeStdTuple();
    new const Handle:conn = SQL_Connect(g_hTuple, errorCode, errorStr, charsmax(errorStr));

    if(conn == Empty_Handle)
    {
        log_amx("Database connection error %i. %s", errorCode, errorStr);

        SQL_FreeHandle(g_hTuple);

        set_fail_state("Failed to connect to database.");
    }

    new Handle:query = SQL_PrepareQuery(conn,
        "CREATE TABLE IF NOT EXISTS players (\
            steamid VARCHAR(64), \
            name VARCHAR(33) NOT NULL, \
            unique_key VARCHAR(33) NOT NULL, \
            ip VARCHAR(45) NOT NULL, \
            country CHAR(2) NOT NULL, \
            time_played INT(11) NOT NULL DEFAULT 0, \
            first_seen INT UNSIGNED NOT NULL DEFAULT UNIX_TIMESTAMP(), \
            last_seen INT UNSIGNED NOT NULL DEFAULT UNIX_TIMESTAMP(), \
            PRIMARY KEY (steamid) \
        ) ENGINE=InnoDB;"
    );

    if(!SQL_Execute(query))
    {
        SQL_QueryError(query, errorStr, charsmax(errorStr));
        log_amx("Failed to execute table creation query for table players. %s", errorStr);
    }
    
    SQL_FreeHandle(query);

    query = SQL_PrepareQuery(conn,
        "CREATE TABLE IF NOT EXISTS players_sessions (\
            steamid VARCHAR(64), \
            joined_time INT UNSIGNED NOT NULL DEFAULT UNIX_TIMESTAMP(), \
            left_time INT UNSIGNED NULL DEFAULT NULL, \
            PRIMARY KEY (steamid) \
        ) ENGINE=InnoDB;"
    );

    if(!SQL_Execute(query))
    {
        SQL_QueryError(query, errorStr, charsmax(errorStr));
        log_amx("Failed to execute table creation query for table players_sessions. %s", errorStr);
    }

    SQL_FreeHandle(conn);
    SQL_FreeHandle(query);
}

public players_generate_unique_keys_cmd()
{
    new errorCode;
    new errorStr[700];
    new const Handle:conn = SQL_Connect(g_hTuple, errorCode, errorStr, charsmax(errorStr));

    if(conn == Empty_Handle)
    {
        log_amx("Database connection error %i. %s", errorCode, errorStr);
        return;
    }

    new Handle:query = SQL_PrepareQuery(conn, "SELECT steamid FROM players WHERE unique_key = ''");

    if(!SQL_Execute(query))
    {
        SQL_QueryError(query, errorStr, charsmax(errorStr));
        log_amx("Failed to execute table creation query. %s", errorStr);
    }


    if(SQL_NumResults(query) == 0)
    {
        log_amx("All players have unique keys");
        SQL_FreeHandle(query);
        return;
    }

    new Handle:updateQuery;
    new unique_key[33];
    new currentSteamID[MAX_AUTHID_LENGTH];
    
    while(SQL_MoreResults(query))
    {
        SQL_ReadResult(query, SQL_FieldNameToNum(query, "steamid"), currentSteamID, charsmax(currentSteamID));

        GenerateRandomKey(unique_key, charsmax(unique_key));
        updateQuery = SQL_PrepareQuery(conn, "UPDATE players SET unique_key = '%s' WHERE steamid = '%s'", unique_key, currentSteamID);

        if(!SQL_Execute(updateQuery))
        {
            SQL_QueryError(updateQuery, errorStr, charsmax(errorStr));
            log_amx("Failed to execute update query for SteamID %s", currentSteamID);
            SQL_NextRow(query);
            continue;
        }

        server_print("Updated %s with %s", currentSteamID, unique_key);

        SQL_FreeHandle(updateQuery);
        SQL_NextRow(query);
    }

    SQL_FreeHandle(query);

    return;
}

public plugin_end()
{
    SQL_FreeHandle(g_hTuple);
}

public client_connect(id)
{
    g_iJoinedTime[id] = get_systime();
    g_iDebouncePanelKeyDBRequestTime[id] = 0;
}

public client_putinserver(id)
{
    if(is_user_bot(id))
        return PLUGIN_CONTINUE;

    new name[MAX_NAME_LENGTH], steamid[MAX_AUTHID_LENGTH], ip[MAX_IP_LENGTH], unique_key[33];

    get_user_name(id, name, charsmax(name));
    get_user_authid(id, steamid, charsmax(steamid));
    get_user_ip(id, ip, charsmax(ip), 1);

    GenerateRandomKey(unique_key, charsmax(unique_key));

    new countryCode[3];
    if(!geoip_code2_ex(ip, countryCode))
    {
        log_amx("Failed to retrieve country code for IP %s (%n)", ip, id);
        formatex(countryCode, charsmax(countryCode), "RO");
    }

    static query[QUERY_SIZE];
    new const len = formatex(query, charsmax(query),
        "INSERT INTO `players` (`steamid`, `name`, `unique_key`, `ip`, `country`, `first_seen`, `last_seen`) \
        VALUES (^"%s^", ^"%s^", '%s', '%s', '%s', UNIX_TIMESTAMP(), UNIX_TIMESTAMP()) \
        ON DUPLICATE KEY UPDATE \
            name = VALUES(name), \
            ip = VALUES(ip), \
            country = VALUES(country), \
            last_seen = UNIX_TIMESTAMP()",
        steamid, name, unique_key, ip, countryCode
    );

    if(len > QUERY_SIZE)
    {
        log_amx("Formatted query length it's bigger than defined query size");
        return PLUGIN_CONTINUE;
    }

    SQL_ThreadQuery(g_hTuple, "FreeHandle", query);

    return PLUGIN_CONTINUE;
}

public client_disconnected(id)
{
    if(is_user_bot(id))
        return PLUGIN_CONTINUE;

    new currentTime = get_systime()
    new playedTime = currentTime - g_iJoinedTime[id];
    
    new authid[MAX_AUTHID_LENGTH];
    get_user_authid(id, authid, charsmax(authid));

    new name[MAX_NAME_LENGTH];
    get_user_name(id, name, charsmax(name));

    SQL_ThreadQuery(g_hTuple, "FreeHandle", fmt("UPDATE players SET name = ^"%s^", time_played = time_played + %i, last_seen = UNIX_TIMESTAMP() WHERE steamid = '%s'", name, playedTime, authid));
    
    return PLUGIN_CONTINUE;
}

public FreeHandle(failstate, Handle:query, error[], errnum, data[], size, Float:queuetime)
{
    if(failstate || errnum)
    {
        log_amx("[LINE: %i] An SQL Error has been encoutered. Error code %i^nError: %s", __LINE__, errnum, error);
        SQL_FreeHandle(query);
        return;
    }

    SQL_FreeHandle(query);
}

stock GenerateRandomKey(output[], len)
{
    static const chars[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    new max = charsmax(chars);

    for (new i = 0; i < len; i++)
    {
        output[i] = chars[random(max)];
    }

    output[len] = 0x00;
}