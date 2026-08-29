void run_worker_identity_tests();
void run_worker_poller_tests();
void run_worker_inbox_tests();
void run_worker_timer_queue_tests();
void run_worker_loop_tests();

int main()
{
    run_worker_identity_tests();
    run_worker_poller_tests();
    run_worker_inbox_tests();
    run_worker_timer_queue_tests();
    run_worker_loop_tests();
}
