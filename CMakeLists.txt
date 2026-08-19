cmake_minimum_required(VERSION 3.25)

project(SnF VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(SNF_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(SNF_ENABLE_THREAD_SANITIZER "Enable ThreadSanitizer" OFF)

if(SNF_ENABLE_SANITIZERS AND SNF_ENABLE_THREAD_SANITIZER)
    message(FATAL_ERROR "Address/Undefined sanitizers cannot be combined with ThreadSanitizer")
endif()

function(snf_configure_target target_name)
    target_compile_features(${target_name} PRIVATE cxx_std_20)

    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target_name} PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )
    endif()

    if(SNF_ENABLE_SANITIZERS)
        if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            message(FATAL_ERROR "SNF_ENABLE_SANITIZERS requires Clang or GCC")
        endif()

        target_compile_options(${target_name} PRIVATE
            -fsanitize=address,undefined
            -fno-omit-frame-pointer
        )
        target_link_options(${target_name} PRIVATE
            -fsanitize=address,undefined
        )
    endif()

    if(SNF_ENABLE_THREAD_SANITIZER)
        if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            message(FATAL_ERROR "SNF_ENABLE_THREAD_SANITIZER requires Clang or GCC")
        endif()

        target_compile_options(${target_name} PRIVATE
            -fsanitize=thread
            -fno-omit-frame-pointer
        )
        target_link_options(${target_name} PRIVATE
            -fsanitize=thread
        )
    endif()
endfunction()

# The game model. It links nothing: no runtime, no sockets, no protocol, no
# threads. Anything it needed from those would fail to link snf_game_tests below,
# which is the wall the compiler keeps for us.
add_library(snf_game STATIC
    src/game/player.cpp
    src/game/zone.cpp
    src/game/party.cpp
    src/game/room.cpp
    src/game/street_progression.cpp
    src/game/product_catalog.cpp
)
target_include_directories(snf_game PUBLIC include)
snf_configure_target(snf_game)

add_library(snf_core STATIC
    src/core/build_info.cpp
    src/protocol/frame_codec.cpp
    src/net/session.cpp
    src/net/socket_options.cpp
    src/net/termination_signal.cpp
    src/net/unique_file_descriptor.cpp
    src/net/tcp_listener.cpp
)
target_include_directories(snf_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
snf_configure_target(snf_core)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(MySQL REQUIRED IMPORTED_TARGET mysqlclient)

add_library(snf_runtime STATIC
    src/runtime/runtime_completion.cpp
    src/runtime/actor_runtime.cpp
)
target_include_directories(snf_runtime
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(snf_runtime PUBLIC Threads::Threads)
snf_configure_target(snf_runtime)

add_library(snf_server_runtime STATIC
    src/server/message_dispatcher.cpp
    src/server/protocol_gateway.cpp
    src/server/command_router.cpp
    src/server/protocol_response_mapper.cpp
    src/server/protocol_player_response_sink.cpp
    src/server/protocol_zone_result_sink.cpp
    src/server/protocol_party_result_sink.cpp
    src/server/protocol_room_result_sink.cpp
    src/server/route_coordinator.cpp
    src/server/party_coordinator.cpp
    src/server/player_actor_binding.cpp
    src/server/player_actor_ingress.cpp
    src/server/player_persistence_service.cpp
    src/server/player_repository.cpp
    src/server/room_actor_binding.cpp
    src/server/room_actor_ingress.cpp
    src/server/player_session_directory.cpp
    src/server/zone_actor_binding.cpp
    src/server/zone_actor_ingress.cpp
    src/server/zone_transition_channel.cpp
    src/server/party_actor_binding.cpp
    src/server/party_actor_ingress.cpp
    src/server/outbound_channel.cpp
    src/server/command_terminal.cpp
    src/server/game_server.cpp
    src/server/tcp_server.cpp
)
target_link_libraries(snf_server_runtime PUBLIC snf_game snf_core snf_runtime)
snf_configure_target(snf_server_runtime)

# The only target that knows MySQL exists. Kept out of snf_server_runtime so that
# linking the server does not drag a database client into every test binary.
add_library(snf_mysql_player_repository STATIC
    src/server/mysql_player_repository.cpp
)
target_link_libraries(snf_mysql_player_repository PUBLIC snf_server_runtime PRIVATE PkgConfig::MySQL)
snf_configure_target(snf_mysql_player_repository)

add_executable(snf_server
    src/server_main.cpp
)
target_link_libraries(snf_server PRIVATE snf_mysql_player_repository)
snf_configure_target(snf_server)

add_library(snf_load_client_runtime STATIC
    src/load/client_connection.cpp
    src/load/load_client.cpp
)
target_link_libraries(snf_load_client_runtime PUBLIC snf_core)
snf_configure_target(snf_load_client_runtime)

add_executable(snf_load_client
    tools/load_client/load_client_main.cpp
)
target_link_libraries(snf_load_client PRIVATE snf_load_client_runtime)
snf_configure_target(snf_load_client)

include(CTest)
if(BUILD_TESTING)
    add_executable(snf_unit_tests
        tests/unit_test_main.cpp
        tests/build_info_test.cpp
        tests/frame_codec_test.cpp
        tests/bounded_queue_test.cpp
        tests/tell_payload_test.cpp
        tests/room_actor_binding_test.cpp
        tests/player_tell_test.cpp
        tests/distribution_test.cpp
        tests/command_router_test.cpp
        tests/protocol_gateway_test.cpp
        tests/protocol_response_mapper_test.cpp
        tests/protocol_zone_result_sink_test.cpp
        tests/protocol_party_result_sink_test.cpp
        tests/protocol_room_result_sink_test.cpp
        tests/route_coordinator_test.cpp
        tests/party_coordinator_test.cpp
        tests/player_response_sink_test.cpp
        tests/outbound_channel_test.cpp
        tests/runtime_boundaries_test.cpp
        tests/player_persistence_service_test.cpp
        tests/player_session_directory_test.cpp
        tests/player_repository_test.cpp
        tests/actor_runtime_test.cpp
        tests/actor_coroutine_test.cpp
        tests/message_dispatcher_test.cpp
        tests/session_test.cpp
        tests/socket_options_test.cpp
        tests/tcp_listener_test.cpp
        tests/unique_file_descriptor_test.cpp
        tests/zone_actor_binding_test.cpp
        tests/zone_transition_channel_test.cpp
        tests/party_actor_binding_test.cpp
    )
    target_link_libraries(snf_unit_tests PRIVATE snf_server_runtime)
    snf_configure_target(snf_unit_tests)

    add_test(NAME snf_unit_tests COMMAND snf_unit_tests)

    # Linked against snf_game alone. If a game translation unit ever reaches for the
    # runtime, a socket or MySQL, this is what stops the build.
    add_executable(snf_game_tests
        tests/game_test_main.cpp
        tests/street_progression_test.cpp
        tests/room_test.cpp
        tests/player_test.cpp
        tests/zone_test.cpp
        tests/party_test.cpp
    )
    target_link_libraries(snf_game_tests PRIVATE snf_game)
    snf_configure_target(snf_game_tests)

    add_test(NAME snf_game_tests COMMAND snf_game_tests)

    # The link gate cannot see an include that resolves but is never linked against,
    # so the layer is also checked by name.
    add_test(NAME snf_game_layer
             COMMAND ${CMAKE_COMMAND} -DGAME_ROOTS=${CMAKE_SOURCE_DIR}/include/snf/game:${CMAKE_SOURCE_DIR}/src/game
                     -P ${CMAKE_SOURCE_DIR}/cmake/check_game_layer.cmake)

    add_executable(snf_integration_tests
        tests/tcp_server_integration_test.cpp
    )
    target_link_libraries(snf_integration_tests
        PRIVATE
            snf_server_runtime
            Threads::Threads
    )
    snf_configure_target(snf_integration_tests)

    add_test(NAME snf_integration_tests COMMAND snf_integration_tests)
    set_tests_properties(snf_integration_tests PROPERTIES TIMEOUT 15)

    add_executable(snf_load_client_integration_tests
        tests/load_client_integration_test.cpp
    )
    target_link_libraries(snf_load_client_integration_tests
        PRIVATE
            snf_load_client_runtime
            snf_server_runtime
            Threads::Threads
    )
    snf_configure_target(snf_load_client_integration_tests)

    add_test(NAME snf_load_client_integration_tests COMMAND snf_load_client_integration_tests)
    set_tests_properties(snf_load_client_integration_tests PROPERTIES TIMEOUT 15)

    add_executable(snf_mysql_integration_tests
        tests/mysql_player_repository_integration_test.cpp
    )
    target_link_libraries(snf_mysql_integration_tests
        PRIVATE
            snf_mysql_player_repository
            snf_load_client_runtime
            Threads::Threads
    )
    snf_configure_target(snf_mysql_integration_tests)

    add_test(NAME snf_mysql_integration_tests COMMAND snf_mysql_integration_tests)
    set_tests_properties(snf_mysql_integration_tests PROPERTIES
        TIMEOUT 30
        SKIP_RETURN_CODE 77
    )
endif()
