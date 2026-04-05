  ./scripts/sweep_mutex_throughput_multi_lock.sh \
    --locks mcs,mcs-tas,flexguard,mcs_tas_simple \
    --profile \
    --outside-ns 350,2800 \
    --critical-ns 35,175,350,700,1400,2800,5600,11200 \
    --repeats 3 \
    --duration-ms 3000 \
    --warmup-duration-ms 1000 \
    --output-root results_new \
    --timeslice-extension require
