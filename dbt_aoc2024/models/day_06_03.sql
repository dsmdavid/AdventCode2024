/*
It was worth trying this as it circumvented recursiveness...
unfortunately, it misses a good number of cases where the 
pattern of the blocks cannot be easily predicted, so 
binning it
*/
{{
  config(
	enabled = false,
	materialized = 'table',
	)
}}
WITH
    raw_data AS (
        {% if var('day06', False) %}
         SELECT 
            regexp_split_to_table('....#.....
.........#
..........
..#.......
.......#..
..........
.#..^.....
........#.
#.........
......#...', '\n') AS raw
        {% else %}
            SELECT raw FROM {{ ref('2024__6') }}
        {% endif %}
	),

    base AS (
        SELECT
            raw,
            ROW_NUMBER() OVER () AS row_id
        FROM raw_data
    ),

    as_array AS (
        SELECT
            raw,
            row_id,
            STRING_SPLIT(raw, '') AS char_list

        FROM base
    ),

    as_chars AS (
        SELECT
            row_id,
            UNNEST(char_list) AS char_,
            GENERATE_SUBSCRIPTS(char_list, 1) AS col_id
        FROM as_array
    ),

    boundaries AS (
        SELECT
            MAX(row_id) AS max_row,
            MAX(col_id) AS max_col
        FROM as_chars
    ),

    starting_point AS (
        SELECT * FROM as_chars
        WHERE char_ = '^'
    ),

    blockers AS (
        SELECT * FROM as_chars
        WHERE char_ = '#'
    ),
    
    all_visited as (
    	select * from {{ ref('day_06') }}
    ),
   
    test_up as (
    --1	going up, 2,3 & 4 must exist, 1 must be in path, 1(r) = 2(r-1), 1(c) = 4(c+1)

    	select
    		blockersa.row_id, blockersa.col_id,
    		blockersb.row_id, blockersb.col_id,
    		blockersc.row_id, blockersc.col_id,
    		all_visited.row_id, all_visited.col_id,
    		all_visited.direction
    	from all_visited
    	inner join blockers as blockersa
    		on all_visited.row_id = blockersa.row_id - 1
    		and all_visited.direction = 'UP'
    	inner join blockers as blockersb
    		on blockersa.col_id = blockersb.col_id + 1
    	inner join blockers as blockersc 
    		on blockersb.row_id = blockersc.row_id + 1
    		and all_visited.col_id = blockersc.col_id + 1
    ),
    test_right as (
    --2	going RIGHT, 1,3 & 4 must exist, 2 must be in path, 2(c) = 3(c+1), 2(r) = 1(r+1)
    	select
    		blockersa.row_id, blockersa.col_id,
    		blockersb.row_id, blockersb.col_id,
    		blockersc.row_id, blockersc.col_id,
    		all_visited.row_id, all_visited.col_id,
    		all_visited.direction
    	from all_visited
    	inner join blockers as blockersa
    		on all_visited.col_id = blockersa.col_id + 1
    		and all_visited.direction = 'RIGHT'
    	inner join blockers as blockersb
    		on blockersa.row_id = blockersb.row_id + 1
    	inner join blockers as blockersc 
    		on blockersb.col_id = blockersc.col_id - 1
    		and all_visited.row_id = blockersc.row_id + 1
    		
    		
    ),
    test_down as (
    --3	going DOWN, 1,2 & 4 must exist, 3 must be in path, 2(c) = 3(c)+1, 3(r) = 4(r+1) 
    	select
    		blockersa.row_id, blockersa.col_id,
    		blockersb.row_id, blockersb.col_id,
    		blockersc.row_id, blockersc.col_id,
    		all_visited.row_id, all_visited.col_id,
    		all_visited.direction
    	from all_visited
    	inner join blockers as blockersa
    		on all_visited.row_id = blockersa.row_id + 1
    		and all_visited.direction = 'DOWN'
    	inner join blockers as blockersb
    		on blockersa.col_id = blockersb.col_id - 1
    	inner join blockers as blockersc 
    		on blockersb.row_id = blockersc.row_id - 1
    		and all_visited.col_id = blockersc.col_id - 1
    ),
    test_left as (
    --4	going LEFT, 1,2 &3 must exist, 4 must be in path, 3(r) = 4(r+1), 4(c) = 1(c-1)        	
		select
	    		blockersa.row_id, blockersa.col_id,
	    		blockersb.row_id, blockersb.col_id,
	    		blockersc.row_id, blockersc.col_id,
	    		all_visited.row_id, all_visited.col_id,
	    		all_visited.direction
	    	from all_visited
	    	inner join blockers as blockersa
	    		on all_visited.col_id = blockersa.col_id - 1
	    		and all_visited.direction = 'LEFT'
	    	inner join blockers as blockersb
	    		on blockersa.row_id = blockersb.row_id - 1
	    	inner join blockers as blockersc 
	    		on blockersb.col_id = blockersc.col_id + 1
	    		and all_visited.row_id = blockersc.row_id - 1
    ), 
    
    all_combinations as (    
    select * from test_up
    union all 
    select * from test_right
    union all
    select * from test_down
    union all     
    select * from test_left
)

select 
	cast(blockersa.row_id as varchar) || '|' || cast(blockersa.col_id as varchar) as 

select count(*) from all_combinations
