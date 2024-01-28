import datetime
import time
import asyncio

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
                                  f"cmty_submissions_{'sprints' if sprints else 'marathon'}", 
                                  ts_col='submitted_time' if daily else None)


async def daily_summary_stats(conn):
    new_user_count = _count_rows_from_table(conn, 'users', ts_col='join_date')
    new_run_count = _count_rows_from_table(conn, 'sprint_runs', ts_col='start_time')
    return  f"New users in the last 24 hours: {new_user_count}" + \
        f"\nNew runs in the last 24 hours: {new_run_count}"


async def potd_status_check(conn):
    prompts_left = _count_consecutive_future_prompts(conn)
    output = f"We currently have {prompts_left} consecutive future prompts left."
    if prompts_left < 7:
        output += "\nWARNING - add more prompts!"
    await asyncio.sleep(10)
    return output


def cmty_submission_stats(conn):
    sprint_total    =   _get_num_cmty_submissions(conn, daily=False,    sprints=True)
    marathon_total  =   _get_num_cmty_submissions(conn, daily=False,    sprints=False)
    sprint_daily    =   _get_num_cmty_submissions(conn, daily=True,     sprints=True)
    marathon_daily  =   _get_num_cmty_submissions(conn, daily=True,     sprints=False)
    return f"Cmty sprints submitted in the last 24 hours: {sprint_daily}" + \
        f"\nCmty marathons submitted in the last 24 hours: {marathon_daily}" + \
        f"\nTotal pending cmty sprints: {sprint_total}" + \
        f"\nTotal pending cmty marathons: {marathon_total}" 
