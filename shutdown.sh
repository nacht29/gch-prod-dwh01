tmux kill-session -t airflow_web 2>/dev/null || true
tmux kill-session -t airflow_sched 2>/dev/null || true
sudo shutdown now