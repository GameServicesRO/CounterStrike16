#if defined _gs_players_const_included
	#endinput
#endif
#define _gs_players_const_included


const NO_SESSION_ID = 0;

enum _:PlayerData
{
    TIME_PLAYED
}

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