#!/bin/bash
RAILWAY="/c/Users/us/AppData/Roaming/npm/node_modules/@railway/cli/bin/railway.exe"
VALUE=$(cat /c/Users/us/Downloads/gen-lang-client-0641615854-901548546697.json | python -c "import sys,json; print(json.dumps(json.load(sys.stdin)))")
$RAILWAY vars set "GOOGLE_SERVICE_ACCOUNT=$VALUE"
