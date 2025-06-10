# Google AddressSanitizer
if(SKYRISE_ENABLE_GOOGLE_ADDRESS_SANITIZER)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls)
    add_link_options(-fsanitize=address)
    message(STATUS "Google ASan enabled")
endif()

# Google LeakSanitizer
if(SKYRISE_ENABLE_GOOGLE_LEAK_SANITIZER)
    add_compile_options(-fsanitize=leak)
    add_link_options(-fsanitize=leak)
    message(STATUS "Google LSan enabled")
endif()

# Google ThreadSanitizer
if(SKYRISE_ENABLE_GOOGLE_THREAD_SANITIZER)
    add_compile_options(-fsanitize=thread)
    add_link_options(-fsanitize=thread)
    message(STATUS "Google TSan enabled")
endif()

# Google UndefinedBehaviorSanitizer
if(SKYRISE_ENABLE_GOOGLE_UNDEFINED_BEHAVIOR_SANITIZER)
    add_compile_options(-fsanitize=undefined)
    add_link_options(-fsanitize=undefined)
    message(STATUS "Google UBSan enabled")
endif()
