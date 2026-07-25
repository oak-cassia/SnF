void run_build_info_tests();
void run_frame_codec_tests();
void run_session_tests();
void run_socket_options_tests();
void run_tcp_listener_tests();
void run_unique_file_descriptor_tests();

int main()
{
    run_build_info_tests();
    run_frame_codec_tests();
    run_session_tests();
    run_socket_options_tests();
    run_tcp_listener_tests();
    run_unique_file_descriptor_tests();
}
