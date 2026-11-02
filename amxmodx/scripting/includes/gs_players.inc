#if defined _gs_players_included
	#endinput
#endif
#define _gs_players_included

#include <gs_players_const>

/**
* Get player total played time.
*
* @param id          		Player index.
*
* @return 					Total played minute or -1 on database state error.
*/
native any:gs_get_player_played_time(const id);