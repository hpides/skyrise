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
