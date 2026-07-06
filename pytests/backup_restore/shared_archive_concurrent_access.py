# This file has been superseded by the refactored layout below.
# Do NOT add tests here.
#
# New structure:
#   shared_archive_base.py          — base class + all helpers
#   shared_archive_happy_path.py    — Happy Path test
#   shared_archive_rebalance.py     — Source rebalance scenario
#   shared_archive_rebalance_dest.py — Destination rebalance scenario
#   shared_archive_failover_src.py  — Source failover scenario
#   shared_archive_failover_dest.py — Destination failover scenario
#   shared_archive_resume.py        — --resume flag test
#   shared_archive_all_services.py  — All-services scenario
#   shared_archive_backup_during_restore.py — Backup during restore
#
# Conf file:
#   conf/backup_restore/shared_archive_all.conf
