with plays as (

  select *
  from {{ ref('plays')}}
  
), columns as (

  select 
    season_code,
    play_key,
    game_date,
    game_id,
    play_type,
    kicker_player_id,
    kicker_player_name,
    is_field_goal_attempt,
    is_field_goal_success,
    is_extra_point_attempt,
    extra_point_result,
    kick_distance
  from plays
  
), calculations as (

  select 
    season_code,
    game_date,
    game_id,
    kicker_player_id as player_id,
    kicker_player_name as player_name,
    count(case when is_field_goal_attempt and is_field_goal_success then 1 end) as kicking_field_goal_made,
    count(case when is_field_goal_attempt and not is_field_goal_success then 1 end) as kicking_field_goal_missed,
    count(case when kick_distance >= 0 and kick_distance < 20 and is_field_goal_attempt and is_field_goal_success then 1 end) as kicking_zero_to_nineteen_field_goal_made,
    count(case when kick_distance >= 0 and kick_distance < 20 and is_field_goal_attempt and not is_field_goal_success then 1 end) as kicking_zero_to_nineteen_field_goal_missed,
    count(case when kick_distance >= 20 and kick_distance < 30 and is_field_goal_attempt and is_field_goal_success then 1 end) as kicking_twenty_to_twenty_nine_field_goal_made,
    count(case when kick_distance >= 20 and kick_distance < 30 and is_field_goal_attempt and not is_field_goal_success then 1 end) as kicking_twenty_to_twenty_nine_field_goal_missed,
    count(case when kick_distance >= 30 and kick_distance < 40 and is_field_goal_attempt and is_field_goal_success then 1 end) as kicking_thirty_to_thirty_nine_field_goal_made,
    count(case when kick_distance >= 30 and kick_distance < 40 and is_field_goal_attempt and not is_field_goal_success then 1 end) as kicking_thirty_to_thirty_nine_field_goal_missed,
    count(case when kick_distance >= 40 and kick_distance < 50 and is_field_goal_attempt and is_field_goal_success then 1 end) as kicking_fourty_to_fourty_nine_field_goal_made,
    count(case when kick_distance >= 40 and kick_distance < 50 and is_field_goal_attempt and not is_field_goal_success then 1 end) as kicking_fourty_to_fourty_nine_field_goal_missed,
    count(case when kick_distance >= 50 and is_field_goal_attempt and is_field_goal_success then 1 end) as kicking_over_fifty_field_goal_made,
    count(case when kick_distance >= 50 and is_field_goal_attempt and not is_field_goal_success then 1 end) as kicking_over_fifty_field_goal_missed,
    count(case when is_extra_point_attempt and extra_point_result = 'good' then 1 end) as kicking_extra_point_made,
    count(case when is_extra_point_attempt and extra_point_result != 'good' then 1 end) as kicking_extra_point_missed,
    sum(case when is_field_goal_attempt and is_field_goal_success then kick_distance end) as kicking_field_goal_yards,
    sum(case when is_field_goal_attempt and is_field_goal_success then greatest(kick_distance - 30,0) end) as kicking_field_goal_yards_over_30_yards
  from columns
  where kicker_player_id is not null
  group by 1,2,3,4,5

)

select *
from calculations