with plays as (

  select *
  from {{ ref('plays') }}
  
), columns as (

  select 
    season_code,
    play_key,
    game_date,
    game_id,
    play_type,
    rusher_player_id,
    rusher_player_name,
    is_first_down_rush,
    is_rush_touchdown,
    yards_gained,
    is_rush_attempt,
    is_two_point_attempt,
    two_point_conv_result,
  from plays
  
), calculations as (

  select 
    season_code,
    game_date,
    game_id,
    rusher_player_id as player_id,
    rusher_player_name as player_name,
    count(case when is_rush_attempt then 1 end) as rushing_attempt,
    count(case when is_rush_touchdown then 1 end) as rushing_touchdown, 
    count(case when is_first_down_rush then 1 end) as rushing_first_down,
    count(case when is_rush_attempt and is_two_point_attempt and two_point_conv_result = 'success' then 1 end) as rushing_two_point_conversion,
    count(case when yards_gained >= 40 then 1 end) as rushing_fourty_plus_yard_rush_bonus,
    count(case when yards_gained >= 40 and is_rush_touchdown then 1 end) as rushing_fourty_plus_yard_touchdown_bonus,
    sum(case when is_rush_attempt then yards_gained end) as rushing_yards
  from columns
  where rusher_player_id is not null
  group by 1,2,3,4,5

)

select *
from calculations