#include <amxmodx>
#include <geoip>
#include <sqlx>
#include <json>
#include <reapi>

#define QUERY_SIZE 1024
#define DEBOUNCE_PANEL_KEY_DB_REQUEST_TIME 2

#pragma semicolon 1

const NO_SESSION_ID = 0;

enum _:PlayerSession
{
    ID,
    JOINED_TIME,
    LEFT_TIME,
    KILLS,
    DEATHS,
    DAMAGE
}

enum DatabaseState
{
    DATA_NOT_RETRIEVED,
    WAITING_DATA,
    DATA_RETRIEVED,
    FAILED_TO_RETRIEVE_DATA
}

new Handle:g_hTuple;

new g_ePlayersSessions[MAX_PLAYERS + 1][PlayerSession];
new DatabaseState:g_ePlayerDatabaseState[MAX_PLAYERS + 1];
new g_iDebouncePanelKeyDBRequestTime[MAX_PLAYERS + 1];

new bool:g_bPendingDisconnect[MAX_PLAYERS + 1];
new g_iPendingDisconnectTime[MAX_PLAYERS + 1];

new HookChain:g_hcTakeDamageHook;
new HookChain:g_hcPlayerKilledHook;

public plugin_init()
{
    register_plugin("[GS] Players", "0.7.9", "lexzor");

    register_concmd("players_generate_unique_keys", "players_generate_unique_keys_cmd");
    register_clcmd("amx_panel_key", "amx_panel_key_cmd");

    g_hcTakeDamageHook = RegisterHookChain(RG_CBasePlayer_TakeDamage, "RG_CBasePlayer_TakeDamage_Post", _:true);
    g_hcPlayerKilledHook = RegisterHookChain(RG_CSGameRules_PlayerKilled, "RG_CSGameRules_PlayerKilled_Post", _:true);
}

public RG_CBasePlayer_TakeDamage_Post(const this, pevInflictor, pevAttacker, Float:flDamage, bitsDamageType)
{
    if(!is_user_connected(pevAttacker))
        return HC_CONTINUE;

    if(g_ePlayerDatabaseState[pevAttacker] != DatabaseState:DATA_RETRIEVED)
        return HC_CONTINUE;

    g_ePlayersSessions[pevAttacker][DAMAGE] += floatround(flDamage);

    return HC_CONTINUE;
}

public RG_CSGameRules_PlayerKilled_Post(const victim, const killer, const inflictor)
{


    if(is_user_connected(victim) && g_ePlayerDatabaseState[victim] == DatabaseState:DATA_RETRIEVED)
    {
        g_ePlayersSessions[victim][DEATHS]++;
    }
    
    if(is_user_connected(killer) && victim != killer && g_ePlayerDatabaseState[killer] == DatabaseState:DATA_RETRIEVED)
    {
        g_ePlayersSessions[killer][KILLS]++;
    }
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

    if(!SQL_SetAffinity("mysql"))
    {
        if(g_hcTakeDamageHook != INVALID_HOOKCHAIN)
            DisableHookChain(g_hcTakeDamageHook);
  
        if(g_hcPlayerKilledHook != INVALID_HOOKCHAIN)
            DisableHookChain(g_hcPlayerKilledHook);

        set_fail_state("Failed to set affinity for SQL. Affinity: mysql");
    }
    
    g_hTuple = SQL_MakeStdTuple();

    if(!SQL_SetCharset(g_hTuple, "utf8mb4"))
    {
        log_amx("Failed to set charset for SQL connection tuple. Charset: utf8mb4");
    }

    new const Handle:conn = SQL_Connect(g_hTuple, errorCode, errorStr, charsmax(errorStr));

    if(conn == Empty_Handle)
    {
        if(g_hcTakeDamageHook != INVALID_HOOKCHAIN)
            DisableHookChain(g_hcTakeDamageHook);

        if(g_hcPlayerKilledHook != INVALID_HOOKCHAIN)
            DisableHookChain(g_hcPlayerKilledHook);

        SQL_FreeHandle(g_hTuple);

        log_amx("Database connection error %i. %s", errorCode, errorStr);
        set_fail_state("Failed to connect to database.");
    }

    new Handle:query = SQL_PrepareQuery(conn,
        "CREATE TABLE IF NOT EXISTS players (\
            steamid VARCHAR(64), \
            name VARCHAR(64) NOT NULL, \
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
            id INT(11) NOT NULL AUTO_INCREMENT, \
            steamid VARCHAR(64), \
            joined_time INT UNSIGNED NOT NULL DEFAULT (UNIX_TIMESTAMP()), \
            left_time INT UNSIGNED NULL DEFAULT NULL, \
            data JSON NULL, \
            PRIMARY KEY (id) \
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
        SQL_FreeHandle(conn);
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

        SQL_FreeHandle(updateQuery);
        SQL_NextRow(query);
    }

    SQL_FreeHandle(query);
    SQL_FreeHandle(conn);
    return;
}

public plugin_end()
{
    SQL_FreeHandle(g_hTuple);
}

public client_connect(id)
{
    g_ePlayersSessions[id][ID] = NO_SESSION_ID;
    g_ePlayersSessions[id][JOINED_TIME] = get_systime();
    g_ePlayersSessions[id][LEFT_TIME] = 0;
    g_ePlayersSessions[id][KILLS] = 0;
    g_ePlayersSessions[id][DEATHS] = 0;
    g_ePlayersSessions[id][DAMAGE] = 0;

    g_ePlayerDatabaseState[id] = DatabaseState:DATA_NOT_RETRIEVED;
    g_iDebouncePanelKeyDBRequestTime[id] = 0;

    g_bPendingDisconnect[id] = false;
    g_iPendingDisconnectTime[id] = 0;
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

    new escapedName[MAX_NAME_LENGTH * 2 + 1];
    SQL_QuoteString(Empty_Handle, escapedName, charsmax(escapedName), name);

    static query[QUERY_SIZE];
    new const len = formatex(query, charsmax(query),
        "INSERT INTO `players` (`steamid`, `name`, `unique_key`, `ip`, `country`, `first_seen`, `last_seen`) \
        VALUES (^"%s^", ^"%s^", '%s', '%s', '%s', UNIX_TIMESTAMP(), UNIX_TIMESTAMP()) \
        ON DUPLICATE KEY UPDATE \
            name = VALUES(name), \
            ip = VALUES(ip), \
            country = VALUES(country), \
            last_seen = UNIX_TIMESTAMP()",
        steamid, escapedName, unique_key, ip, countryCode
    );

    if(len > QUERY_SIZE)
    {
        log_amx("Formatted query length it's bigger than defined query size");
        return PLUGIN_CONTINUE;
    }

    SQL_ThreadQuery(g_hTuple, "FreeHandle", query);

    ManagePlayerSession(id);

    return PLUGIN_CONTINUE;
}

public client_disconnected(id)
{
    if(is_user_bot(id))
        return PLUGIN_CONTINUE;

    new currentTime = get_systime();
    new playedTime = currentTime - g_ePlayersSessions[id][JOINED_TIME];
    
    new authid[MAX_AUTHID_LENGTH];
    get_user_authid(id, authid, charsmax(authid));

    new name[MAX_NAME_LENGTH];
    get_user_name(id, name, charsmax(name));

    new escapedName[MAX_NAME_LENGTH * 2 + 1];
    SQL_QuoteString(Empty_Handle, escapedName, charsmax(escapedName), name);

    SQL_ThreadQuery(g_hTuple, "FreeHandle", fmt("UPDATE players SET name = ^"%s^", time_played = time_played + %i, last_seen = UNIX_TIMESTAMP() WHERE steamid = '%s'", escapedName, playedTime, authid));

    if(g_ePlayerDatabaseState[id] == DatabaseState:DATA_RETRIEVED)
    {
        SavePlayerSession(id);
    }
    else if(g_ePlayerDatabaseState[id] == DatabaseState:WAITING_DATA)
    {
        g_bPendingDisconnect[id] = true;
        g_iPendingDisconnectTime[id] = currentTime;
    }

    return PLUGIN_CONTINUE;
}

ManagePlayerSession(const id)
{
    new authid[MAX_AUTHID_LENGTH];
    get_user_authid(id, authid, charsmax(authid));

    new data[2];
    data[0] = id;
    data[1] = g_ePlayersSessions[id][JOINED_TIME];

    g_ePlayerDatabaseState[id] = DatabaseState:WAITING_DATA;
    SQL_ThreadQuery(g_hTuple, "OnPlayerSessionDataRetrieved", fmt("SELECT * from players_sessions WHERE steamid = '%s' AND left_time > (UNIX_TIMESTAMP() - 60)", authid), data, sizeof(data));
}

public OnPlayerSessionDataRetrieved(failstate, Handle:query, error[], errnum, data[], size, Float:queuetime)
{
    if(failstate || errnum)
    {
        log_amx("[LINE: %i] An SQL Error has been encoutered. Error code %i^nError: %s", __LINE__, errnum, error);
        SQL_FreeHandle(query);
        return;
    }

    new const id = data[0];
    new const joinedTime = data[1];

    // Callback belongs to a previous occupant of this slot, discard it
    if(g_ePlayersSessions[id][JOINED_TIME] != joinedTime)
    {
        SQL_FreeHandle(query);
        return;
    }

    g_ePlayerDatabaseState[id] = DatabaseState:DATA_RETRIEVED;

    if(!is_user_connected(id))
    {
        if(g_bPendingDisconnect[id])
        {
            if(SQL_NumResults(query) == 0)
                CreateNewPlayerSession(id);

            SavePlayerSession(id);
            g_bPendingDisconnect[id] = false;
        }
        SQL_FreeHandle(query);
        return;
    }

    if(SQL_NumResults(query) == 0)
    {
        CreateNewPlayerSession(id);
        goto cleanup;
    }

    if(SQL_NumResults(query) > 1)
    {
        log_amx("Retrieving player session for %N retrieved more than 1 session", id);
        goto cleanup;
    }

    new sessionData[512];
    SQL_ReadResult(query, SQL_FieldNameToNum(query, "data"), sessionData, charsmax(sessionData));

    new JSON:playerSession = json_parse(sessionData, false, false);

    if(playerSession == Invalid_JSON)
    {
        log_amx("Failed to parse JSON for session data");
        goto cleanup;
    }

    g_ePlayersSessions[id][ID] = SQL_ReadResult(query, SQL_FieldNameToNum(query, "id"));
    g_ePlayersSessions[id][JOINED_TIME] = SQL_ReadResult(query, SQL_FieldNameToNum(query, "joined_time"));
    g_ePlayersSessions[id][LEFT_TIME] = SQL_ReadResult(query, SQL_FieldNameToNum(query, "left_time"));

    g_ePlayersSessions[id][KILLS] = json_object_get_number(playerSession, "kills");
    g_ePlayersSessions[id][DEATHS] = json_object_get_number(playerSession, "deaths");
    g_ePlayersSessions[id][DAMAGE] = json_object_get_number(playerSession, "damage");

    json_free(playerSession);

    new authid[MAX_AUTHID_LENGTH];
    get_user_authid(id, authid, charsmax(authid));

    SQL_ThreadQuery(g_hTuple, "FreeHandle", fmt("UPDATE players_sessions SET left_time = 0 WHERE id = %i AND steamid = '%s'", g_ePlayersSessions[id][ID], authid));

    cleanup:
    SQL_FreeHandle(query);
}

CreateNewPlayerSession(const id)
{
    if(!is_user_connected(id) && !is_user_connecting(id))
        return;

    new authid[MAX_AUTHID_LENGTH];
    get_user_authid(id, authid, charsmax(authid));

    new JSON:newSessionData = json_init_object();

    if(newSessionData == Invalid_JSON)
    {
        log_amx("Failed to initialize JSON object");
        return;
    }

    json_object_set_number(newSessionData, "kills", 0);
    json_object_set_number(newSessionData, "deaths", 0);
    json_object_set_number(newSessionData, "damage", 0);

    new queryData[512];
    json_serial_to_string(newSessionData, queryData, charsmax(queryData));
    json_free(newSessionData);

    new errorCode, errorStr[512];
    new const Handle:conn = SQL_Connect(g_hTuple, errorCode, errorStr, charsmax(errorStr));

    if(conn == Empty_Handle)
    {
        if(g_hcTakeDamageHook != INVALID_HOOKCHAIN)
            DisableHookChain(g_hcTakeDamageHook);

        if(g_hcPlayerKilledHook != INVALID_HOOKCHAIN)
            DisableHookChain(g_hcPlayerKilledHook);

        log_amx("Database connection error %i. %s", errorCode, errorStr);
        set_fail_state("Failed to connect to database.");
    }

    new Handle:query = SQL_PrepareQuery(conn, "INSERT INTO `players_sessions` \
        (steamid, joined_time, left_time, data) \
        VALUES (^"%s^", %i, %i, '%s');",
        authid, g_ePlayersSessions[id][JOINED_TIME], 0, queryData
    );

    if(!SQL_Execute(query))
    {
        SQL_QueryError(query, errorStr, charsmax(errorStr));
        log_amx("Failed to create new session for %N. %s", id, errorStr);
        SQL_FreeHandle(query);
        SQL_FreeHandle(conn);
        return;
    }

    g_ePlayersSessions[id][ID] = SQL_GetInsertId(query);

    SQL_FreeHandle(query);
}

SavePlayerSession(const id)
{
    new errorCode, errorStr[512];
    new const Handle:conn = SQL_Connect(g_hTuple, errorCode, errorStr, charsmax(errorStr));

    if(conn == Empty_Handle)
    {
        if(g_hcTakeDamageHook != INVALID_HOOKCHAIN)
            DisableHookChain(g_hcTakeDamageHook);

        if(g_hcPlayerKilledHook != INVALID_HOOKCHAIN)
            DisableHookChain(g_hcPlayerKilledHook);

        log_amx("Database connection error %i. %s", errorCode, errorStr);
        set_fail_state("Failed to connect to database.");
    }

    new authid[MAX_AUTHID_LENGTH];
    get_user_authid(id, authid, charsmax(authid));

    new JSON:data = json_init_object();

    if(data == Invalid_JSON)
    {
        log_amx("Failed to initialize JSON object");
        return;
    }

    json_object_set_number(data, "kills", g_ePlayersSessions[id][KILLS]);
    json_object_set_number(data, "deaths", g_ePlayersSessions[id][DEATHS]);
    json_object_set_number(data, "damage", g_ePlayersSessions[id][DAMAGE]);

    new queryData[512];
    json_serial_to_string(data, queryData, charsmax(queryData));
    json_free(data);

    new leftTime = g_bPendingDisconnect[id] ? g_iPendingDisconnectTime[id] : get_systime();

    new Handle:query = SQL_PrepareQuery(conn, fmt("UPDATE players_sessions SET left_time = %i, data = '%s' WHERE id = %i AND steamid = '%s'", leftTime, queryData, g_ePlayersSessions[id][ID], authid));

    if(!SQL_Execute(query))
    {
        SQL_QueryError(query, errorStr, charsmax(errorStr));
        log_amx("Failed to save session for %N. %s", id, errorStr);
        SQL_FreeHandle(query);
        SQL_FreeHandle(conn);
        return;
    }
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