#pragma once

#include <string>

namespace skyrise {

/*
 * Registers signal handlers for the specified signals.
 *
 * Handler prints stacktrace and raises SIGABRT.
 */
void RegisterSignalHandler();

/*
 * Replaces the signal handling behavior by the default one.
 */
void DeregisterSignalHandler();

/*
 * Returns a string describing the signal number.
 */
std::string SignalToString(int signal_number);

}  // namespace skyrise
