with plays as (

  select *
  from {{ ref('plays') }}
  
), games as (

  select *
  from {{ ref('games') }}
  
), columns as (

  select 
    season_code,
    play_key,
    game_date,
    game_id,
    play_type,
    def_team_code,
    td_team_code,
    fumble_recovery_1_team,
    is_interception,
    is_return_touchdown,
    is_fumble,
    is_fumble_lost,
    is_fumble_forced,
    is_qb_hit,
    is_sack,
    is_tackled_for_loss,
    is_assist_tackle, 
    is_solo_tackle,
    is_safety,
    is_punt_blocked,
    is_field_goal_success,
    is_field_goal_attempt,
    field_goal_result,
    extra_point_result,
    yards_gained,
    return_yards
  from plays
  
), calculations as (

  select 
    season_code,
    game_date,
    game_id,
    def_team_code as player_name,
    def_team_code as player_id,
    count(case when is_qb_hit then 1 end) defence_qb_hit,
    count(case when is_sack then 1 end) as defence_sack,
    count(case when def_team_code = td_team_code then 1 end) as defensive_touchdown,    -- this is missing kickoff returns
    count(case when is_fumble_forced then 1 end) as defensive_fumble_forced,
    count(case when is_interception then 1 end) as defensive_interception,
    count(case when is_fumble_lost then 1 end) as defensive_fumble_recovery,
    count(case when is_tackled_for_loss then 1 end) as defensive_tackle_for_lost,
    count(case when is_assist_tackle then 1 end) as defensive_assist_tackle,
    count(case when is_solo_tackle then 1 end) as defensive_solo_tackle,
    count(case when is_solo_tackle or is_assist_tackle then 1 end) as defensive_tackle,
    count(case when is_safety then 1 end) as defensive_safety,
    count(case when is_punt_blocked or field_goal_result = 'blocked' or extra_point_result = 'blocked' then 1 end) as defensive_kick_blocked,
    sum(case when is_sack then abs(yards_gained) end) as defence_sack_yards,
    sum(case when is_interception then return_yards end) as defence_interception_return_yards,
    sum(case when is_fumble_lost then return_yards end) as defence_fumble_return_yards,
    sum(case when not is_field_goal_success and is_field_goal_attempt then return_yards end) as defence_missed_field_goal_return_yards,
    sum(case when is_punt_blocked or field_goal_result = 'blocked' or extra_point_result = 'blocked' then return_yards end) as defensive_blocked_kick_return_yards,
    sum(yards_gained) as defence_yards_allowed
  from columns
  where def_team_code is not null
  group by 1,2,3,4,5

), joined as (

  select 
    calculations.*,
    case 
      when calculations.player_id = home_team_code then away_score
      when calculations.player_id = away_team_code then home_score
    end as defence_points_allowed
  from calculations
  inner join games
    on calculations.game_id = games.game_id

), buckets as (

  select
    *,
    'DEF' as position_code,
    case when defence_points_allowed = 0 then 1 end as defence_zero_points_allowed,
    case when defence_points_allowed >= 1 and defence_points_allowed < 7 then 1 end as defence_one_to_six_points_allowed,
    case when defence_points_allowed >= 7 and defence_points_allowed < 14 then 1 end as defence_seven_to_thirteen_points_allowed,
    case when defence_points_allowed >= 14 and defence_points_allowed < 21 then 1 end as defence_fourteen_to_twenty_points_allowed,
    case when defence_points_allowed >= 21 and defence_points_allowed < 28 then 1 end as defence_twenty_one_to_twenty_seven_points_allowed,
    case when defence_points_allowed >= 28 and defence_points_allowed < 35 then 1 end as defence_twenty_eight_to_thirty_four_points_allowed,
    case when defence_points_allowed >= 35 then 1 end as defence_thirty_five_plus_points_allowed,
    case when defence_yards_allowed >= 0 and defence_yards_allowed < 100 then 1 end as defence_1_to_99_yards_allowed,
    case when defence_yards_allowed >= 100 and defence_yards_allowed < 200 then 1 end as defence_100_to_199_yards_allowed,
    case when defence_yards_allowed >= 200 and defence_yards_allowed < 300 then 1 end as defence_200_to_299_yards_allowed,
    case when defence_yards_allowed >= 300 and defence_yards_allowed < 350 then 1 end as defence_300_to_349_yards_allowed,
    case when defence_yards_allowed >= 350 and defence_yards_allowed < 400 then 1 end as defence_350_to_399_yards_allowed,
    case when defence_yards_allowed >= 400 and defence_yards_allowed < 450 then 1 end as defence_400_to_449_yards_allowed,
    case when defence_yards_allowed >= 450 and defence_yards_allowed < 500 then 1 end as defence_450_to_499_yards_allowed,
    case when defence_yards_allowed >= 500 and defence_yards_allowed < 550 then 1 end as defence_500_to_549_yards_allowed,
    case when defence_yards_allowed >= 550 then 1 end as defence_550_plus_yards_allowed
  from joined

)

select *
from buckets