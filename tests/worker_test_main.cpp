void run_worker_identity_tests();
void run_worker_poller_tests();
void run_worker_inbox_tests();
void run_worker_timer_queue_tests();
void run_worker_loop_tests();
void run_worker_connection_tests();
void run_worker_actor_tests();
void run_worker_metrics_tests();
void run_worker_progress_tests();
void run_worker_latency_histogram_tests();
void run_worker_watchdog_tests();

int main()
{
    run_worker_identity_tests();
    run_worker_poller_tests();
    run_worker_inbox_tests();
    run_worker_timer_queue_tests();
    run_worker_loop_tests();
    run_worker_connection_tests();
    run_worker_actor_tests();
    run_worker_metrics_tests();
    run_worker_progress_tests();
    run_worker_latency_histogram_tests();
    run_worker_watchdog_tests();
}
