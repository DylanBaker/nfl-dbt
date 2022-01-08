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
    air_yards,
    yards_after_catch,
    is_first_down_pass,
    is_incomplete_pass,
    is_complete_pass,
    is_interception,
    is_pass_touchdown,
    is_return_touchdown,
    is_pass_attempt,
    is_sack,
    is_two_point_attempt,
    two_point_conv_result,
    passer_player_id,
    passer_player_name
  from plays
  
), calculations as (

  select 
    season_code,
    passer_player_name as player_name,
    passer_player_id as player_id,
    game_date,
    game_id,
    count(case when is_pass_attempt and not is_sack then 1 end) as passing_attempted,
    count(case when is_complete_pass then 1 end) as passing_completed,
    count(case when is_pass_attempt and not is_complete_pass and not is_sack then 1 end) as passing_incomplete,
    count(case when is_pass_touchdown then 1 end) as passing_touchdown,
    count(case when is_interception then 1 end) as passing_interception,
    count(case when is_interception and is_return_touchdown then 1 end) as passing_pick_six,
    count(case when is_sack then 1 end) as passing_sack,
    count(case when is_first_down_pass then 1 end) as passing_first_down,
    count(case when is_pass_attempt and is_two_point_attempt and two_point_conv_result = 'success' then 1 end) as passing_two_point_conversion,
    count(case when air_yards + yards_after_catch >= 40 then 1 end) as passing_fourty_plus_yard_completion_bonus,
    count(case when air_yards + yards_after_catch >= 40 and is_pass_touchdown then 1 end) as passing_fourty_plus_yard_touchdown_bonus,
    sum(case when is_complete_pass then air_yards + yards_after_catch end) as passing_yards
  from columns
  where passer_player_id is not null
  group by 1,2,3,4,5

)

select *
from calculations
