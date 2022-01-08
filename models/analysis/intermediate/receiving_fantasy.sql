with plays as (

  select *
  from {{ ref('plays') }}
  
), players as (

  select *
  from {{ ref('teams_players') }}
  
), columns as (

  select 
    plays.season_code,
    plays.play_key,
    plays.game_date,
    plays.game_id,
    plays.play_type,
    plays.receiver_player_id,
    plays.receiver_player_name,
    plays.air_yards,
    plays.yards_after_catch,
    plays.is_first_down_pass,
    plays.is_complete_pass,
    plays.is_pass_touchdown,
    plays.is_two_point_attempt,
    plays.two_point_conv_result,
    players.position_code
  from plays
  left join players
    on plays.season_code = players.season_code
    and plays.off_team_code = players.team_code
    and plays.receiver_player_id = players.player_id
  
), calculations as (

  select 
    season_code,
    game_date,
    game_id,
    receiver_player_id as player_id,
    receiver_player_name as player_name,
    count(case when is_complete_pass then 1 end) as receiving_reception,
    count(case when is_pass_touchdown then 1 end) as receiving_touchdown,
    count(case when is_first_down_pass then 1 end) as receiving_first_down,
    count(case when is_complete_pass and is_two_point_attempt and two_point_conv_result = 'success' then 1 end) as receiving_two_point_conversion,
    count(case when is_complete_pass and air_yards + yards_after_catch >= 0 and air_yards + yards_after_catch < 5 then 1 end) as receiving_zero_to_four_yard_bonus,
    count(case when is_complete_pass and air_yards + yards_after_catch >= 5 and air_yards + yards_after_catch < 10 then 1 end) as receiving_five_to_nine_yard_bonus,
    count(case when is_complete_pass and air_yards + yards_after_catch >= 10 and air_yards + yards_after_catch < 20 then 1 end) as receiving_ten_to_nineteen_yard_bonus,
    count(case when is_complete_pass and air_yards + yards_after_catch >= 20 and air_yards + yards_after_catch < 30 then 1 end) as receiving_twenty_to_twenty_nine_yard_bonus,
    count(case when is_complete_pass and air_yards + yards_after_catch >= 30 and air_yards + yards_after_catch < 40 then 1 end) as receiving_thirty_to_thirty_nine_yard_bonus,
    count(case when is_complete_pass and air_yards + yards_after_catch >= 40 then 1 end) as receiving_fourty_plus_yard_bonus,
    count(case when is_complete_pass and air_yards + yards_after_catch >= 40 and is_pass_touchdown then 1 end) as receiving_fourty_plus_yard_touchdown_bonus,
    count(case when is_complete_pass and position_code = 'WR' then 1 end) as receiving_wr_reception_bonus,
    count(case when is_complete_pass and position_code = 'TE' then 1 end) as receiving_te_reception_bonus,
    count(case when is_complete_pass and position_code = 'RB' then 1 end) as receiving_rb_reception_bonus,
    sum(case when is_complete_pass then air_yards + yards_after_catch end) as receiving_yards
  from columns
  where receiver_player_id is not null
  group by 1,2,3,4,5

)

select *
from calculations