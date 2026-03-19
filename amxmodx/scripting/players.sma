#include <amxmodx>
#include <sqlx>

#define QUERY_SIZE 1024

#pragma semicolon 1

new Handle:g_hTuple;

new g_iJoinedTime[MAX_PLAYERS + 1];

public plugin_init()
{
    register_plugin("[GS] Players", "0.1", "lexzor");

    new errorCode;
    new errorStr[700];

    g_hTuple = SQL_MakeStdTuple();
    new const Handle:conn = SQL_Connect(g_hTuple, errorCode, errorStr, charsmax(errorStr));

    if(conn == Empty_Handle)
    {
        log_amx("Database connection error %i. %s", errorCode, errorStr);

        if(g_hTuple != Empty_Handle)
            SQL_FreeHandle(g_hTuple);

        set_fail_state("Failed to connect to database.");
    }

    new Handle:query = SQL_PrepareQuery(conn,
        "CREATE TABLE IF NOT EXISTS players (\
            steamid VARCHAR(34), \
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
        log_amx("Failed to execute table creation query. %s", errorStr);
    }

    SQL_FreeHandle(conn);
    SQL_FreeHandle(query);

    register_concmd("players_generate_unique_keys", "players_generate_unique_keys_cmd");
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

    new Handle:updateQuery;
    new unique_key[33];
    new currentSteamID[MAX_AUTHID_LENGTH];

    if(SQL_NumResults(query) == 0)
    {
        log_amx("All players have unique keys");
        return;
    }

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

        SQL_NextRow(query);
    }

    return;
}

public plugin_end()
{
    SQL_FreeHandle(g_hTuple);
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

    static query[QUERY_SIZE];
    new const len = formatex(query, charsmax(query),
        "INSERT INTO `players` (`steamid`, `name`, `unique_key`, `ip`, `country`, `first_seen`, `last_seen`) \
        VALUES (^"%s^", ^"%s^", '%s', '%s', 'RO', UNIX_TIMESTAMP(), UNIX_TIMESTAMP()) \
        ON DUPLICATE KEY UPDATE \
            name = VALUES(name), \
            ip = VALUES(ip), \
            country = VALUES(country), \
            last_seen = UNIX_TIMESTAMP()",
        steamid, name, unique_key, ip
    );

    g_iJoinedTime[id] = get_systime();

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

    new authid[MAX_AUTHID_LENGTH];
    get_user_authid(id, authid, charsmax(authid));

    new name[MAX_NAME_LENGTH];
    get_user_name(id, name, charsmax(name));

    SQL_ThreadQuery(g_hTuple, "FreeHandle", fmt("UPDATE players SET name = '%s', time_played = time_played + %i, last_seen = UNIX_TIMESTAMP() WHERE steamid = '%s'", name, get_systime() - g_iJoinedTime[id], authid));
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