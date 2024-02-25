# discord-bot
Discord bot used to automate text reports to the wikispeedruns discord server

## Run instructions

0. Clone the repo and create a venv. 
```
python -m venv env
```

1. Activate the venv with
```
./env/Scripts/Activate.ps1
```

2. Install necessary packages with
```
pip install -r requirements.txt
```

3. Set discord channel ID and discord bot token environment variables:

On linux:
```
export CHANNEL_ID='<ID_VALUE>'
export BOT_TOKEN='<BOT_TOKEN>'
```

On windows powershell
```
$Env:CHANNEL_ID = "<ID_VALUE>"
$Env:BOT_TOKEN = "<BOT_TOKEN>"
```

4. In the same cmd window, launch the script.
```
python .\wikispeedruns_bot_main.py 
```