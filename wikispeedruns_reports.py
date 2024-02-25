import datetime


from pymysql.cursors import DictCursor


def _count_consecutive_future_prompts(conn):
    query = """
    SELECT active_start FROM sprint_prompts WHERE active_start > DATE(NOW()) ORDER BY active_start
    """
    with conn.cursor(cursor=DictCursor) as cursor:
        cursor.execute(query)
        dates = [row['active_start'].date() for row in cursor.fetchall()]  # Convert datetime to date
        conn.commit()

    if not dates: return 0

    count = 0
    current_date = datetime.date.today() + datetime.timedelta(days=1)
    for date in dates:
        if date != current_date:
            break
        count += 1
        current_date += datetime.timedelta(days=1)
    return count


def _count_rows_from_table(conn, table_name, ts_col=None):
    query = f"""
    SELECT COUNT(*) AS count FROM {table_name}
    """
    if ts_col:
        query += f" WHERE {ts_col} >= DATE_SUB(NOW(), INTERVAL 24 HOUR)"
    with conn.cursor(cursor=DictCursor) as cursor:
        cursor.execute(query)
        count = cursor.fetchone()['count']
        conn.commit()
        return count


def _get_num_cmty_submissions(conn, daily=False, sprints=True):
    return _count_rows_from_table(conn,
                                  f"cmty_pending_prompts_{'sprints' if sprints else 'marathon'}",
                                  ts_col='submitted_time' if daily else None)


async def daily_summary_stats(conn):
    new_user_count = _count_rows_from_table(conn, 'users', ts_col='join_date')
    new_run_count = _count_rows_from_table(conn, 'sprint_runs', ts_col='start_time')
    return  f"New users in the last 24 hours: **{new_user_count}**" + \
        f"\nNew runs in the last 24 hours: **{new_run_count}**"


async def potd_status_check(conn):
    prompts_left = _count_consecutive_future_prompts(conn)
    output = f"We currently have **{prompts_left}** consecutive future prompts left."
    if prompts_left < 7:
        output += "\n***WARNING - add more prompts!***"
    return output


async def cmty_submission_stats(conn):
    sprint_total    =   _get_num_cmty_submissions(conn, daily=False,    sprints=True)
    marathon_total  =   _get_num_cmty_submissions(conn, daily=False,    sprints=False)
    sprint_daily    =   _get_num_cmty_submissions(conn, daily=True,     sprints=True)
    marathon_daily  =   _get_num_cmty_submissions(conn, daily=True,     sprints=False)
    return f"Cmty sprints submitted in the last 24 hours: **{sprint_daily}**" + \
        f"\nCmty marathons submitted in the last 24 hours: **{marathon_daily}**" + \
        f"\nTotal pending cmty sprints: **{sprint_total}**" + \
        f"\nTotal pending cmty marathons: **{marathon_total}**"

def _get_leaderboard_query(
    prompt_id,
    only_completed=False,
    first_per_user=False,
    join_user=False,
    select_str="count(*) AS count"
):

    where_conditions = ['runs.start_time >= prompt.active_start',
                        'runs.start_time <= prompt.active_end',
                        f'runs.prompt_id = {prompt_id}']
    if only_completed:
        where_conditions.append("runs.finished = 1")

    query = f"""
    SELECT {select_str} FROM wikipedia_speedruns.sprint_runs AS runs
    JOIN wikipedia_speedruns.sprint_prompts AS prompt
    ON runs.prompt_id = prompt.prompt_id
    """

    if first_per_user:
        query += f"""
            LEFT JOIN (
                SELECT MIN(run_id) AS run_id
                FROM wikipedia_speedruns.sprint_runs
                WHERE prompt_id={prompt_id}
                GROUP BY user_id
            ) AS first_runs
            ON first_runs.run_id = runs.run_id
            """
        where_conditions.append("first_runs.run_id IS NOT NULL")
    if join_user:
        query += f"""
        LEFT JOIN wikipedia_speedruns.users AS users ON runs.user_id = users.user_id
        """

    query += f"WHERE {' AND '.join(where_conditions)}"

    return query



async def daily_prompt_summary(conn):
    query = """
    SELECT prompt_id, start FROM wikipedia_speedruns.sprint_prompts
    WHERE active_end >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
		AND active_end < UTC_TIMESTAMP()
    ORDER BY active_end DESC
    LIMIT 1
    """
    with conn.cursor(cursor=DictCursor) as cursor:
        cursor.execute(query)
        prompt = cursor.fetchone()
        prompt_id = prompt['prompt_id']
        start_article = prompt['start']
        conn.commit()

    num_runs                = _get_leaderboard_query(prompt_id, select_str='count(*) as res',
                                                     only_completed=False, first_per_user=True)
    num_completed_runs      = _get_leaderboard_query(prompt_id, select_str='count(*) as res',
                                                     only_completed=True, first_per_user=True)
    average_time            = _get_leaderboard_query(prompt_id, select_str='avg(play_time) as res',
                                                     only_completed=True, first_per_user=True)
    average_path_length     = _get_leaderboard_query(prompt_id, select_str="JSON_LENGTH(runs.`path`, '$.path') AS res",
                                                     only_completed=True, first_per_user=True)

    with conn.cursor(cursor=DictCursor) as cursor:
        output = []
        for item in [num_runs, num_completed_runs, average_time, average_path_length]:
            cursor.execute(item)
            output.append(cursor.fetchone()['res'])
            conn.commit()
        num_runs, num_completed_runs, average_time, average_path_length = tuple(output)

    top = _get_leaderboard_query(prompt_id, select_str="username, play_time",
                                 only_completed=True, first_per_user=True, join_user=True) + \
        " ORDER BY play_time LIMIT 3"
    with conn.cursor(cursor=DictCursor) as cursor:
        cursor.execute(top)
        top = cursor.fetchall()
        conn.commit()

    completion_rate = float(num_completed_runs) / num_runs * 100 if num_runs else 0
    completion_rate_comment = "What is going on?!"
    if completion_rate < 5:
        completion_rate_comment = "This prompt may have been a mistake..."
    elif completion_rate < 25:
        completion_rate_comment = "Oooof."
    elif completion_rate < 50:
        completion_rate_comment = "Yikes!"
    elif completion_rate < 75:
        completion_rate_comment = "Not bad!"

    average_time_comment = "At least someone finished... Maybe?"
    if average_time < 30:
        average_time_comment = "Now that's a speedrun!"
    elif average_time < 60:
        average_time_comment = "Slow down speedsters!"
    elif average_time < 120:
        average_time_comment = "Impressive!"
    elif average_time < 180:
        average_time_comment = "Not bad!"
    elif average_time < 400:
        average_time_comment = "Could be better!"

    average_path_length_comment = ""
    if average_path_length < 5:
        average_path_length_comment = "Was this prompt too easy?"


    return f"\n***Prompt {prompt_id}: \"{start_article}\"***\n\n" + \
        f"**{num_runs}** wikispeedrunners attempted this prompt, while **{num_completed_runs}** crossed the finish line on their first try. \n" + \
        f"That's an overall first try completion rate of **{completion_rate:.1f}%**. {completion_rate_comment}\n\n" + \
        f"Of those who finished, the average time is **{average_time:.2f}** seconds. {average_time_comment}\n" + \
        f"The average path length was **{int(average_path_length)}**. {average_path_length_comment}\n\n" + \
        f"""Congrats to our top {len(top)}: {', '.join([f'**{row["username"]}** *({row["play_time"]:.2f} seconds)*' for row
        in top])}!\n\n""" + \
        "Go challenge today's prompt when you're ready at** https://wikispeedruns.com/ **. See you again in 24 hours!"
