cmake_minimum_required(VERSION 3.25)

project(SnF VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(SNF_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

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
endfunction()

add_library(snf_core STATIC
    src/core/build_info.cpp
    src/protocol/frame_codec.cpp
    src/net/session.cpp
    src/net/unique_file_descriptor.cpp
    src/net/tcp_listener.cpp
)
target_include_directories(snf_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
snf_configure_target(snf_core)

add_executable(snf_server
    src/server_main.cpp
)
target_link_libraries(snf_server PRIVATE snf_core)
snf_configure_target(snf_server)

add_executable(snf_load_client
    tools/load_client/load_client_main.cpp
)
target_link_libraries(snf_load_client PRIVATE snf_core)
snf_configure_target(snf_load_client)

include(CTest)
if(BUILD_TESTING)
    add_executable(snf_unit_tests
        tests/unit_test_main.cpp
        tests/build_info_test.cpp
        tests/frame_codec_test.cpp
        tests/session_test.cpp
        tests/tcp_listener_test.cpp
        tests/unique_file_descriptor_test.cpp
    )
    target_link_libraries(snf_unit_tests PRIVATE snf_core)
    snf_configure_target(snf_unit_tests)

    add_test(NAME snf_unit_tests COMMAND snf_unit_tests)
endif()
