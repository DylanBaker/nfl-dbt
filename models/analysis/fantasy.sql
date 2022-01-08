{{ config(materialized='table') }}

with defence as (

    select *
    from {{ ref('defence_fantasy') }}

), kicking as (

    select *
    from {{ ref('kicking_fantasy') }}

), passing as (

    select *
    from {{ ref('passing_fantasy') }}

), receiving as (

    select *
    from {{ ref('receiving_fantasy') }}

), rushing as (

    select *
    from {{ ref('rushing_fantasy') }}

), players as (

    select *
    from {{ ref('teams_players') }}

), games as (

    select *
    from {{ ref('games') }}

{% set pairs = [
    ['join_one','passing','kicking'],
    ['join_two','join_one','receiving'],
    ['join_three','join_two','rushing'],
    ['join_four','join_three','defence'],
] %}

{% for p in pairs %}

), {{ p[0] }} as (

    select
        coalesce({{ p[1] }}.season_code, {{ p[2] }}.season_code) as season_code,
        coalesce({{ p[1] }}.player_name, {{ p[2] }}.player_name) as player_name,
        coalesce({{ p[1] }}.player_id, {{ p[2] }}.player_id) as player_id,
        coalesce({{ p[1] }}.game_date, {{ p[2] }}.game_date) as game_date,
        coalesce({{ p[1] }}.game_id, {{ p[2] }}.game_id) as game_id,
        {{ p[1] }}.* except (season_code, player_name, player_id, game_date, game_id),
        {{ p[2] }}.* except (season_code, player_name, player_id, game_date, game_id)
    from {{ p[1] }}
    full outer join {{ p[2] }} 
        on {{ p[1] }}.season_code = {{ p[2] }}.season_code
        and {{ p[1] }}.player_name = {{ p[2] }}.player_name
        and {{ p[1] }}.player_id = {{ p[2] }}.player_id
        and {{ p[1] }}.game_date = {{ p[2] }}.game_date
        and {{ p[1] }}.game_id = {{ p[2] }}.game_id

{% endfor %}

), distinct_players as (

    select distinct
        player_id,
        player_name_full,
        season_code,
        position_code
    from players

), most_recent_position as (

    select distinct
        player_id,
        last_value(position_code) over (partition by player_id order by season_nbr asc) as position_code
    from players
    

), positions as (

    select 
        coalesce(join_four.position_code, distinct_players.position_code, most_recent_position.position_code, 'K') as position_code,
        join_four.* except (position_code),
        coalesce(distinct_players.player_name_full, join_four.player_name) as player_name_full,
        games.week_nbr
    from join_four
    left join distinct_players
        on join_four.player_id = distinct_players.player_id
        and join_four.season_code = distinct_players.season_code
    left join most_recent_position
        on join_four.player_id = most_recent_position.player_id
    left join games
        on join_four.game_id = games.game_id
    where join_four.season_code like 'REG%'

)

select *
from positions
