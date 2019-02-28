#!/bin/sh
# Adicionar no crontab:
# sudo crontab -e
# Ex: rodar a cada 2 horas
# 0 */2 * * * su -u ivan "/home/ivan/ajna/virasana/periodic_updates.sh"
# touch /var/log/virasana/periodic_updates.log
# chown -U ivan /var/log/virasana/periodic_updates.log
/home/ivan/ajna/virasana/virasana-venv/bin/python virasana/scripts/periodic_updates.py >> /var/log/virasana/periodic_updates.log 2&>1
