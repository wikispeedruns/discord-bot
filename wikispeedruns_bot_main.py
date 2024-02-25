import json
import pymysql
import os
import asyncio
import datetime

import discord
from discord.ext import commands, tasks

import wikispeedruns_reports as reports


intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='>', intents=intents)

channel_id = int(os.getenv('CHANNEL_ID'))
bot_token = os.getenv('BOT_TOKEN')

# Report intervals
# 'DAILY':   everyday at 00:00:00 UTC
# 'DEBUG_x': DEBUG_ONLY - every x seconds

bot_reports = [
    
    ## ADMIN REPORTS
    # 1. DAILY - report number of new users and new accounts in the last 24 hours
    # 2. DAILY - report number of days we have POTDs left for, and how many unused POTDs are in the queue. Add warning if < 7 days left
    # 3. DAILY - report number of new cmty submissions in the last 24 hours + total number of cmty submissions
    
    ## COMMUNITY REPORTS
    # 1. DAILY - announce winners of the previous POTD, Along with stats:
    #   - number of runs + completed runs
    #   - completion rate
    #   - average time
    #   - average path length
    #   If new POTD is active, announce prompt, link to home page
    
    {
        'name': 'daily_summary_stats',
        'target_channel': channel_id,
        'interval': 'DAILY',
        'func': reports.daily_summary_stats
    },
    {
        'name': 'potd_status_check',
        'target_channel': channel_id,
        'interval': 'DAILY',
        'func': reports.potd_status_check
    },
    {
        'name': 'daily_cmty_submission_stats',
        'target_channel': channel_id,
        'interval': 'DAILY',
        'func': reports.cmty_submission_stats
    },
    {
        'name': 'Prompt of the day summary!',
        'target_channel': channel_id,
        'interval': 'DAILY',
        'func': reports.daily_prompt_summary
    },
]

async_tasks = []

@bot.event
async def on_ready():
    conn = get_database()
    for report in bot_reports:
        @tasks.loop(seconds=1)
        async def func(report=report):
            now = datetime.datetime.utcnow()
            local_now = datetime.datetime.now()
            if report['interval'] == 'DAILY':
                next_midnight = datetime.datetime(now.year, now.month, now.day) + datetime.timedelta(days=1)
                time_to_wait = (next_midnight - now).total_seconds()
            if report['interval'].startswith('DEBUG_'):
                time_to_wait = int(report['interval'].split('_')[1]) - 1
            
            print(f'report name: {report['name']}')
            print(f'cur utc time: {now}, cur local time: {local_now}')
            print(f'next report in {time_to_wait} seconds')
            print(f'at utc time: {now + datetime.timedelta(seconds=time_to_wait)}, at local time: {local_now + datetime.timedelta(seconds=time_to_wait)}')

            await asyncio.sleep(time_to_wait)
            channel = bot.get_channel(report['target_channel'])
            if channel:
                res = await report['func'](conn)
                report_str = f"\n**{report['name']}**\n{res}"
                print(report_str)
                await channel.send(report_str)
        func.before_loop(bot.wait_until_ready)
        async_tasks.append(func)
    for task in async_tasks:
        task.start()
    print('Bot is ready, tasks starting...')


DEFAULT_DB_NAME='wikipedia_speedruns'
def get_database(db_name=DEFAULT_DB_NAME):
    config = json.load(open("./config/default.json"))
    try:
        config.update(json.load(open("./config/prod.json")))
    except FileNotFoundError:
        pass
    return pymysql.connect(
        user=config["MYSQL_USER"],
        host=config["MYSQL_HOST"],
        password=config["MYSQL_PASSWORD"],
        database=db_name
    )

if __name__ == '__main__':
    bot.run(bot_token)
