void run_build_info_tests();
void run_bounded_queue_tests();
void run_distribution_tests();
void run_frame_codec_tests();
void run_command_router_tests();
void run_protocol_gateway_tests();
void run_protocol_response_mapper_tests();
void run_player_effect_sink_tests();
void run_runtime_boundary_tests();
void run_player_actor_tests();
void run_actor_runtime_tests();
void run_message_dispatcher_tests();
void run_session_tests();
void run_socket_options_tests();
void run_tcp_listener_tests();
void run_unique_file_descriptor_tests();

int main()
{
    run_build_info_tests();
    run_bounded_queue_tests();
    run_distribution_tests();
    run_frame_codec_tests();
    run_command_router_tests();
    run_protocol_gateway_tests();
    run_protocol_response_mapper_tests();
    run_player_effect_sink_tests();
    run_runtime_boundary_tests();
    run_player_actor_tests();
    run_actor_runtime_tests();
    run_message_dispatcher_tests();
    run_session_tests();
    run_socket_options_tests();
    run_tcp_listener_tests();
    run_unique_file_descriptor_tests();
}
