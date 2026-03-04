#!/bin/bash
# Auto-run stock screener at 11pm and 7am HKT daily

while true; do
    HOUR=$(date +%H)
    MIN=$(date +%M)
    
    # 11pm HKT = 15:00 UTC
    # 7am HKT = 23:00 UTC previous day
    
    if [ "$HOUR" = "15" ] && [ "$MIN" = "00" ]; then
        echo "Running 11pm HKT scan (no history)..."
        /home/linuxbrew/.linuxbrew/bin/python3 /home/admin/stock/run_all.py --no-save-history
        sleep 70
    fi
    
    if [ "$HOUR" = "23" ] && [ "$MIN" = "00" ]; then
        echo "Running 7am HKT scan (with history)..."
        /home/linuxbrew/.linuxbrew/bin/python3 /home/admin/stock/run_all.py --save-history
        sleep 70
    fi
    
    sleep 30
done
