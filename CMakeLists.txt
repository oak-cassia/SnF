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

include(CTest)
